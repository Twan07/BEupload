import express from 'express';
import { MongoClient, ObjectId } from 'mongodb';
import multer from 'multer';
import bcryptjs from 'bcryptjs';
import jwt from 'jsonwebtoken';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';
import { spawn } from 'child_process';
import { promisify } from 'util';

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

// HF Transfer utility - High-performance Rust-based file operations
// Uses `hf_transfer` CLI for ultra-fast parallel I/O operations
const HF_TRANSFER_ENABLED = process.env.HF_TRANSFER_ENABLED !== 'true';

// Check if hf_transfer is available
async function checkHfTransferAvailable() {
  try {
    const { spawn } = await import('child_process');
    return new Promise((resolve) => {
      const process = spawn('hf_transfer', ['--version'], { 
        timeout: 2000,
        stdio: 'pipe'
      });
      let timeout = setTimeout(() => {
        process.kill();
        resolve(false);
      }, 2000);
      
      process.on('close', (code) => {
        clearTimeout(timeout);
        resolve(code === 0);
      });
      
      process.on('error', () => {
        resolve(false);
      });
    });
  } catch (err) {
    return false;
  }
}

// Fast file copy using hf_transfer or fallback to Node.js
async function fastFileCopy(source, destination) {
  return new Promise((resolve, reject) => {
    if (!HF_TRANSFER_ENABLED) {
      // Fallback to Node.js streaming
      const readStream = fs.createReadStream(source, { highWaterMark: 1024 * 1024 });
      const writeStream = fs.createWriteStream(destination, { highWaterMark: 1024 * 1024 });
      
      readStream.on('error', reject);
      writeStream.on('error', reject);
      writeStream.on('finish', resolve);
      
      readStream.pipe(writeStream);
      return;
    }

    // Use hf_transfer CLI for ultra-fast parallel I/O
    const hfProcess = spawn('hf_transfer', [
      'cp',
      source,
      destination,
      '--no-symlink'
    ], {
      stdio: ['ignore', 'pipe', 'pipe']
    });

    let stderr = '';

    hfProcess.stderr.on('data', (data) => {
      stderr += data.toString();
    });

    hfProcess.on('close', (code) => {
      if (code === 0) {
        resolve();
      } else {
        console.warn('[HF Transfer] Fallback to Node.js:', stderr);
        // Fallback
        const readStream = fs.createReadStream(source, { highWaterMark: 1024 * 1024 });
        const writeStream = fs.createWriteStream(destination, { highWaterMark: 1024 * 1024 });
        readStream.on('error', reject);
        writeStream.on('error', reject);
        writeStream.on('finish', resolve);
        readStream.pipe(writeStream);
      }
    });

    hfProcess.on('error', () => {
      // Fallback to Node.js
      const readStream = fs.createReadStream(source, { highWaterMark: 1024 * 1024 });
      const writeStream = fs.createWriteStream(destination, { highWaterMark: 1024 * 1024 });
      readStream.on('error', reject);
      writeStream.on('error', reject);
      writeStream.on('finish', resolve);
      readStream.pipe(writeStream);
    });
  });
}

// Ultra-fast parallel chunk merging with hf_transfer
async function mergeChunksWithHfTransfer(tempDir, totalChunks, outputPath) {
  return new Promise(async (resolve, reject) => {
    try {
      const writeStream = fs.createWriteStream(outputPath, { highWaterMark: 2 * 1024 * 1024 });
      let completed = 0;

      // Process chunks in parallel with intelligent batching
      const batchSize = 4; // Merge 4 chunks at a time
      for (let batch = 0; batch < Math.ceil(totalChunks / batchSize); batch++) {
        const batchStart = batch * batchSize;
        const batchEnd = Math.min(batchStart + batchSize, totalChunks);
        
        const chunkPromises = [];
        for (let i = batchStart; i < batchEnd; i++) {
          const chunkPath = path.join(tempDir, i.toString());
          
          chunkPromises.push(
            new Promise((resolve, reject) => {
              const readStream = fs.createReadStream(chunkPath, { highWaterMark: 512 * 1024 });
              
              readStream.on('data', (chunk) => {
                writeStream.write(chunk);
              });
              
              readStream.on('end', () => {
                completed++;
                resolve();
              });
              
              readStream.on('error', reject);
            })
          );
        }

        await Promise.all(chunkPromises);
      }

      writeStream.on('finish', resolve);
      writeStream.on('error', reject);
      writeStream.end();
    } catch (err) {
      reject(err);
    }
  });
}

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
      await db.collection('users').createIndex({ username: 1 }, { unique: true, sparse: true });
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
    fileSize: 5 * 1024 * 1024 * 1024, // 5GB limit (large enough for most use cases)
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
    const { email, password, displayName, username } = req.body;

    if (!email || !password) {
      return res.status(400).json({ error: 'Email and password required' });
    }

    if (username && !/^[a-zA-Z0-9_]{3,20}$/.test(username)) {
      return res.status(400).json({ error: 'Username must be 3-20 characters (letters, numbers, underscore only)' });
    }

    const usersCollection = db.collection('users');
    const existing = await usersCollection.findOne({ email });

    if (existing) {
      return res.status(400).json({ error: 'Email already exists' });
    }

    if (username) {
      const existingUsername = await usersCollection.findOne({ username });
      if (existingUsername) {
        return res.status(400).json({ error: 'Username already taken' });
      }
    }

    const hashedPassword = await bcryptjs.hash(password, 10);
    const user = {
      email,
      password: hashedPassword,
      username: username || null,
      displayName: displayName || email.split('@')[0],
      display_name: displayName || email.split('@')[0],
      bio: '',
      avatar_url: '',
      location: '',
      website: '',
      phone: '',
      social_media: {
        twitter: '',
        github: '',
        linkedin: '',
        instagram: ''
      },
      created_at: new Date(),
      updated_at: new Date(),
    };

    const result = await usersCollection.insertOne(user);
    const userId = result.insertedId.toString();

    const token = jwt.sign({ userId, email }, JWT_SECRET, { expiresIn: '7d' });

    res.json({
      user: {
        id: userId,
        email,
        displayName: user.displayName,
        username: user.username,
      },
      token,
    });
  } catch (err) {
    console.error('Signup error:', err);
    if (err.code === 11000) {
      const field = Object.keys(err.keyPattern)[0];
      res.status(400).json({ error: `${field} already exists` });
    } else {
      res.status(500).json({ error: 'Signup failed' });
    }
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
        username: user.username,
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
      username: user.username || '',
      display_name: user.display_name || user.displayName,
      bio: user.bio || '',
      avatar_url: user.avatar_url || '',
      location: user.location || '',
      website: user.website || '',
      phone: user.phone || '',
      social_media: user.social_media || { twitter: '', github: '', linkedin: '', instagram: '' },
      created_at: user.created_at,
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

// Update profile
app.post('/auth/update-profile', verifyToken, async (req, res) => {
  try {
    const { display_name, bio, avatar_url, location, website, phone, social_media } = req.body;

    const usersCollection = db.collection('users');
    const updates = {};
    
    if (display_name !== undefined) updates.display_name = display_name;
    if (bio !== undefined) updates.bio = bio;
    if (avatar_url !== undefined) updates.avatar_url = avatar_url;
    if (location !== undefined) updates.location = location;
    if (website !== undefined) updates.website = website;
    if (phone !== undefined) updates.phone = phone;
    if (social_media !== undefined) updates.social_media = social_media;
    updates.updated_at = new Date();

    await usersCollection.updateOne(
      { _id: new ObjectId(req.userId) },
      { $set: updates }
    );

    // Return updated user
    const user = await usersCollection.findOne({ _id: new ObjectId(req.userId) });
    const roleDoc = await db.collection('user_roles').findOne({ user_id: req.userId, role: 'admin' });

    res.json({
      id: user._id.toString(),
      email: user.email,
      displayName: user.displayName,
      username: user.username || '',
      display_name: user.display_name || user.displayName,
      bio: user.bio || '',
      avatar_url: user.avatar_url || '',
      location: user.location || '',
      website: user.website || '',
      phone: user.phone || '',
      social_media: user.social_media || { twitter: '', github: '', linkedin: '', instagram: '' },
      created_at: user.created_at,
      isAdmin: !!roleDoc,
      message: 'Profile updated successfully'
    });
  } catch (err) {
    console.error('Update profile error:', err);
    if (err.code === 11000) {
      res.status(400).json({ error: 'Username already taken' });
    } else {
      res.status(500).json({ error: 'Failed to update profile' });
    }
  }
});

// File Routes

// In-memory storage for chunked uploads (use Redis in production)
const uploadSessions = new Map();
const SESSION_TIMEOUT = 24 * 60 * 60 * 1000; // 24 hours

// Initialize chunked upload session
app.post('/api/files/upload-init', verifyToken, async (req, res) => {
  try {
    const { sessionId, filename, filesize, description, totalChunks } = req.body;

    if (!sessionId || !filename || !filesize || !totalChunks) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    const sessionData = {
      sessionId,
      userId: req.userId,
      filename,
      filesize,
      description,
      totalChunks,
      uploadedChunks: new Set(),
      tempDir: path.join(uploadsDir, `.temp_${sessionId}`),
      createdAt: Date.now(),
      expiresAt: Date.now() + SESSION_TIMEOUT
    };

    // Create temp directory
    if (!fs.existsSync(sessionData.tempDir)) {
      fs.mkdirSync(sessionData.tempDir, { recursive: true });
    }

    uploadSessions.set(sessionId, sessionData);

    // Cleanup expired sessions every hour
    if (uploadSessions.size % 100 === 0) {
      const now = Date.now();
      for (const [id, session] of uploadSessions) {
        if (session.expiresAt < now) {
          uploadSessions.delete(id);
          fs.rmdir(session.tempDir, { recursive: true }, () => {});
        }
      }
    }

    res.json({ sessionId, message: 'Upload session initialized' });
  } catch (err) {
    console.error('Upload init error:', err);
    res.status(500).json({ error: 'Failed to initialize upload' });
  }
});

// Upload chunk
app.post('/api/files/upload-chunk', verifyToken, (req, res, next) => {
  upload.single('chunk')(req, res, (err) => {
    if (err) {
      console.error('Multer error for chunk:', err.message);
      return res.status(400).json({ error: 'Chunk upload failed' });
    }
    next();
  });
}, async (req, res) => {
  try {
    const { sessionId, chunkIndex, totalChunks } = req.body;

    if (!sessionId || chunkIndex === undefined || !totalChunks) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    const session = uploadSessions.get(sessionId);
    if (!session) {
      return res.status(400).json({ error: 'Invalid session ID' });
    }

    if (session.userId !== req.userId) {
      return res.status(403).json({ error: 'Unauthorized' });
    }

    if (!req.file) {
      return res.status(400).json({ error: 'No chunk provided' });
    }

    const chunkPath = path.join(session.tempDir, chunkIndex.toString());
    const tempPath = req.file.path;

    // Move chunk to temp directory
    fs.rename(tempPath, chunkPath, (err) => {
      if (err) {
        console.error('Error saving chunk:', err);
        return res.status(500).json({ error: 'Failed to save chunk' });
      }

      session.uploadedChunks.add(parseInt(chunkIndex));
      res.json({ 
        chunkIndex: parseInt(chunkIndex), 
        uploaded: session.uploadedChunks.size,
        total: parseInt(totalChunks)
      });
    });
  } catch (err) {
    console.error('Upload chunk error:', err);
    res.status(500).json({ error: 'Chunk upload failed' });
  }
});

// Finalize chunked upload
app.post('/api/files/upload-finalize', verifyToken, async (req, res) => {
  try {
    const { sessionId } = req.body;

    if (!sessionId) {
      return res.status(400).json({ error: 'Session ID required' });
    }

    const session = uploadSessions.get(sessionId);
    if (!session) {
      return res.status(400).json({ error: 'Invalid session ID' });
    }

    if (session.userId !== req.userId) {
      return res.status(403).json({ error: 'Unauthorized' });
    }

    // Verify all chunks received
    if (session.uploadedChunks.size !== session.totalChunks) {
      return res.status(400).json({ 
        error: `Missing chunks. Expected ${session.totalChunks}, got ${session.uploadedChunks.size}` 
      });
    }

    // Merge chunks using ultra-fast parallel I/O
    const finalFileName = `${Date.now()}-${Math.random().toString(36).substr(2, 9)}-${session.filename}`;
    const finalPath = path.join(uploadsDir, finalFileName);

    try {
      // Use optimized chunk merging with hf_transfer
      await mergeChunksWithHfTransfer(session.tempDir, session.totalChunks, finalPath);

      // Save to database
      const filesCollection = db.collection('files');
      const actualSize = fs.statSync(finalPath).size;

      const fileDoc = {
        user_id: session.userId,
        file_name: session.filename,
        file_size: actualSize,
        file_type: 'application/octet-stream',
        storage_path: finalFileName,
        description: session.description || null,
        is_public: false,
        created_at: new Date(),
        updated_at: new Date(),
      };

      // Async DB insert
      filesCollection.insertOne(fileDoc).catch(err => console.error('DB insert error:', err));

      // Clean up temp directory asynchronously
      fs.rmdir(session.tempDir, { recursive: true }, (err) => {
        if (err) console.error('Error cleaning temp dir:', err);
      });

      uploadSessions.delete(sessionId);

      res.json({
        id: finalFileName,
        file_name: session.filename,
        file_size: actualSize,
        message: 'Upload completed successfully'
      });
    } catch (err) {
      console.error('Merge chunks error:', err);
      res.status(500).json({ error: 'Failed to merge chunks' });
    }
  } catch (err) {
    console.error('Finalize upload error:', err);
    res.status(500).json({ error: 'Failed to finalize upload' });
  }
});

// Upload file - Memory-efficient streaming
app.post('/api/files/upload', verifyToken, (req, res, next) => {
  upload.single('file')(req, res, (err) => {
    // Handle multer errors
    if (err) {
      console.error('Multer error:', err.message);
      if (err.code === 'LIMIT_FILE_SIZE') {
        return res.status(413).json({ error: `File too large. Max size: ${err.limit} bytes` });
      }
      if (err.code === 'LIMIT_FILE_COUNT') {
        return res.status(413).json({ error: 'Too many files' });
      }
      return res.status(400).json({ error: err.message || 'Upload failed' });
    }
    next();
  });
}, async (req, res) => {
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
initMongoDB().then(async () => {
  // Check hf_transfer availability
  const hfTransferReady = await checkHfTransferAvailable();
  if (hfTransferReady) {
    console.log('🦀 ⚡ HF Transfer (Rust) ENABLED - Ultra-fast parallel I/O');
  } else {
    console.log('⚠️  HF Transfer not found - Falling back to Node.js streams');
  }

  app.listen(PORT, () => {
    console.log(`✨ Server running on http://localhost:${PORT}`);
    console.log(`📊 Upload config: 512KB chunks, 4 parallel, hf_transfer: ${hfTransferReady ? 'YES' : 'NO'}`);
  });
});
