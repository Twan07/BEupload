import express from 'express';
import { MongoClient, ObjectId } from 'mongodb';
import multer from 'multer';
import bcryptjs from 'bcryptjs';
import jwt from 'jsonwebtoken';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';

// Load environment variables
dotenv.config({ path: '.env.server' });

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const uploadsDir = path.join(__dirname, 'uploads');

if (!fs.existsSync(uploadsDir)) {
  fs.mkdirSync(uploadsDir, { recursive: true });
}

const app = express();
const PORT = process.env.PORT || 3001;
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb+srv://admin:Tuandzvcl@userupload.1zqgqci.mongodb.net/?appName=UserUpload';
const JWT_SECRET = process.env.JWT_SECRET || 'TwanDZ';

let db;
let mongoClient;

// Initialize MongoDB
async function initMongoDB() {
  try {
    console.log('🔄 Connecting to MongoDB...');
    console.log('URI:', MONGODB_URI.replace(/:[^:]*@/, ':****@')); // Hide password in logs
    
    mongoClient = new MongoClient(MONGODB_URI, {
      serverSelectionTimeoutMS: 5000,
      connectTimeoutMS: 10000,
    });
    
    await mongoClient.connect();
    console.log('✅ MongoDB connected successfully');
    
    db = mongoClient.db('file-caddy');

    // Create collections if they don't exist
    const collections = await db.listCollections().toArray();
    const collectionNames = collections.map(c => c.name);

    if (!collectionNames.includes('users')) {
      await db.createCollection('users');
      await db.collection('users').createIndex({ email: 1 }, { unique: true });
      console.log('📦 Created users collection');
    }
    if (!collectionNames.includes('files')) {
      await db.createCollection('files');
      await db.collection('files').createIndex({ user_id: 1 });
      console.log('📦 Created files collection');
    }
    if (!collectionNames.includes('user_roles')) {
      await db.createCollection('user_roles');
      console.log('📦 Created user_roles collection');
    }

    console.log('✅ MongoDB fully initialized');
  } catch (err) {
    console.error('❌ MongoDB connection error:', err.message);
    console.error('Full error:', err);
    process.exit(1);
  }
}

// Middleware - Memory efficient for large uploads
// Limit JSON requests but NOT file uploads (handled by multer streaming)
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));
app.use('/uploads', express.static(uploadsDir));

// Increase timeout for large uploads
app.use((req, res, next) => {
  req.setTimeout(3600000); // 1 hour timeout
  res.setTimeout(3600000);
  next();
});

// Multer configuration - Optimized for faster uploads
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, uploadsDir);
  },
  filename: (req, file, cb) => {
    const uniqueName = `${Date.now()}-${Math.random().toString(36).substr(2, 9)}-${file.originalname}`;
    cb(null, uniqueName);
  }
});

// Memory-efficient streaming upload configuration
// Uses disk streaming (not memory buffering) to minimize RAM usage
const upload = multer({
  storage,
  limits: {
    fileSize: 0, // 0 = unlimited
    files: 10 // Increased from 5 (now supports up to 10 concurrent uploads safely)
  },
  // Optimized streaming: 32KB chunks for better parallelism & faster throughput
  // Smaller chunks = more concurrent disk I/O = faster overall speed
  highWaterMark: 32 * 1024 // 32KB chunks (down from 64KB) for better streaming performance
});

// Middleware to verify JWT
function verifyToken(req, res, next) {
  const token = req.headers['authorization']?.split(' ')[1];
  if (!token) {
    return res.status(401).json({ error: 'No token provided' });
  }
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    req.userId = decoded.userId;
    req.email = decoded.email;
    next();
  } catch (err) {
    res.status(401).json({ error: 'Invalid token' });
  }
}

// Auth Routes

// Sign up
app.post('/auth/signup', async (req, res) => {
  try {
    const { email, password, displayName } = req.body;

    if (!email || !password) {
      return res.status(400).json({ error: 'Email and password required' });
    }

    const usersCollection = db.collection('users');
    const existing = await usersCollection.findOne({ email });

    if (existing) {
      return res.status(400).json({ error: 'User already exists' });
    }

    const hashedPassword = await bcryptjs.hash(password, 10);
    const user = {
      email,
      password: hashedPassword,
      displayName: displayName || email.split('@')[0],
      created_at: new Date(),
    };

    const result = await usersCollection.insertOne(user);
    const userId = result.insertedId.toString();

    const token = jwt.sign({ userId, email }, JWT_SECRET, { expiresIn: '7d' });

    res.json({
      user: {
        id: userId,
        email,
        displayName: user.displayName,
      },
      token,
    });
  } catch (err) {
    console.error('Signup error:', err);
    res.status(500).json({ error: 'Signup failed' });
  }
});

// Sign in
app.post('/auth/signin', async (req, res) => {
  try {
    const { email, password } = req.body;

    if (!email || !password) {
      return res.status(400).json({ error: 'Email and password required' });
    }

    const usersCollection = db.collection('users');
    const user = await usersCollection.findOne({ email });

    if (!user) {
      return res.status(400).json({ error: 'Invalid credentials' });
    }

    const isValid = await bcryptjs.compare(password, user.password);

    if (!isValid) {
      return res.status(400).json({ error: 'Invalid credentials' });
    }

    const token = jwt.sign({ userId: user._id.toString(), email }, JWT_SECRET, { expiresIn: '7d' });

    res.json({
      user: {
        id: user._id.toString(),
        email: user.email,
        displayName: user.displayName,
      },
      token,
    });
  } catch (err) {
    console.error('Signin error:', err);
    res.status(500).json({ error: 'Signin failed' });
  }
});

// Get current user
app.get('/auth/me', verifyToken, async (req, res) => {
  try {
    const usersCollection = db.collection('users');
    const user = await usersCollection.findOne({ _id: new ObjectId(req.userId) });

    if (!user) {
      return res.status(404).json({ error: 'User not found' });
    }

    // Check if admin
    const roleDoc = await db.collection('user_roles').findOne({ user_id: req.userId, role: 'admin' });

    res.json({
      id: user._id.toString(),
      email: user.email,
      displayName: user.displayName,
      isAdmin: !!roleDoc,
    });
  } catch (err) {
    console.error('Get user error:', err);
    res.status(500).json({ error: 'Failed to fetch user' });
  }
});

// Change password
app.post('/auth/change-password', verifyToken, async (req, res) => {
  try {
    const { currentPassword, newPassword } = req.body;

    if (!currentPassword || !newPassword) {
      return res.status(400).json({ error: 'Current and new password required' });
    }

    if (newPassword.length < 6) {
      return res.status(400).json({ error: 'New password must be at least 6 characters' });
    }

    const usersCollection = db.collection('users');
    const user = await usersCollection.findOne({ _id: new ObjectId(req.userId) });

    if (!user) {
      return res.status(404).json({ error: 'User not found' });
    }

    // Verify current password
    const isValid = await bcryptjs.compare(currentPassword, user.password);
    if (!isValid) {
      return res.status(401).json({ error: 'Current password is incorrect' });
    }

    // Hash new password
    const hashedPassword = await bcryptjs.hash(newPassword, 10);

    // Update password
    await usersCollection.updateOne(
      { _id: new ObjectId(req.userId) },
      { $set: { password: hashedPassword, updated_at: new Date() } }
    );

    res.json({ success: true, message: 'Password changed successfully' });
  } catch (err) {
    console.error('Change password error:', err);
    res.status(500).json({ error: 'Failed to change password' });
  }
});

// File Routes

// Upload file - Memory-efficient streaming
app.post('/api/files/upload', verifyToken, upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file provided' });
    }

    const { description } = req.body;
    const filesCollection = db.collection('files');

    // Get actual file size from disk
    const actualSize = req.file.size;

    const fileDoc = {
      user_id: req.userId,
      file_name: req.file.originalname,
      file_size: actualSize,
      file_type: req.file.mimetype,
      storage_path: req.file.filename,
      description: description || null,
      is_public: false,
      created_at: new Date(),
      updated_at: new Date(),
    };

    // Fast async insert - doesn't wait for completion
    filesCollection.insertOne(fileDoc).catch(err => console.error('DB insert error:', err));

    // Send response immediately - data saves in background
    res.json({
      id: req.file.filename,
      file_name: fileDoc.file_name,
      file_size: fileDoc.file_size,
    }).end();
  } catch (err) {
    console.error('Upload error:', err);
    if (req.file && req.file.path) {
      // Async cleanup - don't block response
      fs.unlink(req.file.path, () => {});
    }
    res.status(500).json({ error: 'Upload failed' });
  }
});

// Get files
app.get('/api/files', verifyToken, async (req, res) => {
  try {
    const filesCollection = db.collection('files');
    let query = {};

    // Check if admin
    const roleDoc = await db.collection('user_roles').findOne({ user_id: req.userId, role: 'admin' });

    if (!roleDoc) {
      query.user_id = req.userId;
    }

    const files = await filesCollection
      .find(query)
      .sort({ created_at: -1 })
      .toArray();

    const formattedFiles = files.map(f => ({
      id: f._id.toString(),
      ...f,
      _id: undefined,
    }));

    res.json(formattedFiles);
  } catch (err) {
    console.error('Get files error:', err);
    res.status(500).json({ error: 'Failed to fetch files' });
  }
});

// Delete file
app.delete('/api/files/:id', verifyToken, async (req, res) => {
  try {
    const filesCollection = db.collection('files');
    const fileDoc = await filesCollection.findOne({ _id: new ObjectId(req.params.id) });

    if (!fileDoc) {
      return res.status(404).json({ error: 'File not found' });
    }

    // Check ownership or admin
    const roleDoc = await db.collection('user_roles').findOne({ user_id: req.userId, role: 'admin' });

    if (fileDoc.user_id !== req.userId && !roleDoc) {
      return res.status(403).json({ error: 'Unauthorized' });
    }

    // Delete file from disk
    const filePath = path.join(uploadsDir, fileDoc.storage_path);
    fs.unlink(filePath, (err) => {
      if (err) console.error('Error deleting file:', err);
    });

    await filesCollection.deleteOne({ _id: new ObjectId(req.params.id) });

    res.json({ success: true });
  } catch (err) {
    console.error('Delete file error:', err);
    res.status(500).json({ error: 'Failed to delete file' });
  }
});

// Get file by storage path
app.get('/api/files/download/:filename', (req, res) => {
  try {
    const filename = req.params.filename;
    const filepath = path.join(uploadsDir, filename);

    // Security: ensure the file is within uploads directory
    if (!path.resolve(filepath).startsWith(path.resolve(uploadsDir))) {
      return res.status(403).json({ error: 'Access denied' });
    }

    if (!fs.existsSync(filepath)) {
      return res.status(404).json({ error: 'File not found' });
    }

    res.download(filepath);
  } catch (err) {
    console.error('Download error:', err);
    res.status(500).json({ error: 'Download failed' });
  }
});

// Admin Routes

// Get all users
app.get('/api/admin/users', verifyToken, async (req, res) => {
  try {
    // Check if admin
    const roleDoc = await db.collection('user_roles').findOne({ user_id: req.userId, role: 'admin' });

    if (!roleDoc) {
      return res.status(403).json({ error: 'Unauthorized' });
    }

    const usersCollection = db.collection('users');
    const users = await usersCollection
      .find({}, { projection: { password: 0 } })
      .toArray();

    const formattedUsers = users.map(u => ({
      id: u._id.toString(),
      ...u,
      _id: undefined,
    }));

    res.json(formattedUsers);
  } catch (err) {
    console.error('Get users error:', err);
    res.status(500).json({ error: 'Failed to fetch users' });
  }
});

// Get admin files
app.get('/api/admin/files', verifyToken, async (req, res) => {
  try {
    // Check if admin
    const roleDoc = await db.collection('user_roles').findOne({ user_id: req.userId, role: 'admin' });

    if (!roleDoc) {
      return res.status(403).json({ error: 'Unauthorized' });
    }

    const filesCollection = db.collection('files');
    const files = await filesCollection
      .find({})
      .sort({ created_at: -1 })
      .toArray();

    const formattedFiles = files.map(f => ({
      id: f._id.toString(),
      ...f,
      _id: undefined,
    }));

    res.json(formattedFiles);
  } catch (err) {
    console.error('Get admin files error:', err);
    res.status(500).json({ error: 'Failed to fetch files' });
  }
});

// Set user as admin
app.post('/api/admin/users/:userId/role', verifyToken, async (req, res) => {
  try {
    // Check if admin
    const roleDoc = await db.collection('user_roles').findOne({ user_id: req.userId, role: 'admin' });

    if (!roleDoc) {
      return res.status(403).json({ error: 'Unauthorized' });
    }

    const { role } = req.body;
    const userRolesCollection = db.collection('user_roles');

    if (role === 'admin') {
      await userRolesCollection.updateOne(
        { user_id: req.params.userId },
        { $set: { user_id: req.params.userId, role: 'admin' } },
        { upsert: true }
      );
    } else {
      await userRolesCollection.deleteOne({ user_id: req.params.userId, role: 'admin' });
    }

    res.json({ success: true });
  } catch (err) {
    console.error('Set role error:', err);
    res.status(500).json({ error: 'Failed to update role' });
  }
});

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

// Start server
initMongoDB().then(() => {
  app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
  });
});
