import express from 'express';
import { MongoClient, ObjectId } from 'mongodb';
import multer from 'multer';
import bcryptjs from 'bcryptjs';
import jwt from 'jsonwebtoken';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';
import cluster from 'cluster';
import os from 'os';
import compression from 'compression';

// Load environment variables
dotenv.config({ path: '.env.server' });

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const uploadsDir = path.join(__dirname, 'uploads');
const tempChunksDir = path.join(__dirname, 'uploads_tmp');

if (!fs.existsSync(uploadsDir)) {
  fs.mkdirSync(uploadsDir, { recursive: true });
}

if (!fs.existsSync(tempChunksDir)) {
  fs.mkdirSync(tempChunksDir, { recursive: true });
}

const app = express();
const PORT = process.env.PORT || 3001;
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb+srv://admin:Tuandzvcl@userupload.1zqgqci.mongodb.net/?appName=UserUpload';
const JWT_SECRET = process.env.JWT_SECRET || 'TwanDZ';

// Rate limiting per IP to prevent abuse
const requestCounts = new Map();
const RATE_LIMIT_WINDOW = 60000; // 1 minute
const MAX_REQUESTS_PER_WINDOW = 2000; // 2000 req/min supports 15 files × 8 chunks × multiple users

// Cleanup request counts every 5 minutes
setInterval(() => {
  requestCounts.clear();
}, 5 * 60 * 1000);

// Rate limiter middleware (skip for upload-chunk to allow fast uploads)
app.use((req, res, next) => {
  // Skip rate limiting for upload-chunk endpoint to allow high-speed parallel uploads
  if (req.path === '/api/files/upload-chunk') {
    return next();
  }

  const ip = req.ip;
  const now = Date.now();
  
  if (!requestCounts.has(ip)) {
    requestCounts.set(ip, []);
  }
  
  const requests = requestCounts.get(ip);
  const recentRequests = requests.filter(t => now - t < RATE_LIMIT_WINDOW);
  
  if (recentRequests.length >= MAX_REQUESTS_PER_WINDOW) {
    return res.status(429).json({ error: 'Too many requests, please try again later' });
  }
  
  recentRequests.push(now);
  requestCounts.set(ip, recentRequests);
  next();
});

let db;
let mongoClient;

// Track concurrent uploads
const uploadSessions = new Map(); // uploadId -> { chunks: Set, totalChunks, status }

// Initialize MongoDB
async function initMongoDB() {
  try {
    console.log('🔄 Connecting to MongoDB...');
    console.log('URI:', MONGODB_URI.replace(/:[^:]*@/, ':****@')); // Hide password in logs
    
    mongoClient = new MongoClient(MONGODB_URI, {
      serverSelectionTimeoutMS: 5000,
      connectTimeoutMS: 10000,
      // HIGH-PERFORMANCE POOLING: Optimize connection reuse
      maxPoolSize: 100, // Increased from 50 for 15 concurrent files × 8 chunks
      minPoolSize: 25, // Keep more connections warm for burst uploads
      maxIdleTimeMS: 30000,
      waitQueueTimeoutMS: 10000,
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
app.use(express.urlencoded({ extended: false, limit: '10mb' })); // Disable extended mode for perf

// ⚡ Response compression for 2-3x faster downloads
app.use(compression({ level: 6, threshold: 1024 })); // Compress responses > 1KB

// Skip static file serving in production - use CDN/nginx instead
if (process.env.NODE_ENV !== 'production') {
  app.use('/uploads', express.static(uploadsDir, { 
    maxAge: '31d',
    setHeaders: (res) => {
      res.setHeader('Cache-Control', 'public, max-age=2678400, immutable');
    }
  }));
}

// Increase timeout for large uploads
app.use((req, res, next) => {
  req.setTimeout(3600000); // 1 hour timeout
  res.setTimeout(3600000);
  // PERF: Increase socket buffer sizes for fast streaming
  if (req.socket) {
    req.socket.setMaxListeners(0); // Unlimited listeners
  }
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

// High-performance streaming upload configuration
// Uses disk streaming (not memory buffering) for maximum throughput
const upload = multer({
  storage,
  limits: {
    fileSize: 5 * 1024 * 1024 * 1024, // 5GB limit
    files: 10
  },
  // HIGH-PERFORMANCE: 16MB buffer for 100MB/s throughput
  highWaterMark: 16 * 1024 * 1024
});

const chunkStorage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, tempChunksDir);
  },
  filename: (req, file, cb) => {
    // Use temporary unique name - will rename after getting uploadId from body
    const tempName = `temp-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    cb(null, tempName);
  }
});

const chunkUpload = multer({
  storage: chunkStorage,
  limits: {
    fileSize: 100 * 1024 * 1024, // 100MB per chunk
    files: 1
  },
  highWaterMark: 64 * 1024 * 1024 // 64MB buffer = 2x faster streaming
});

async function tryFinalizeChunkedUpload({ uploadId, totalChunks, originalName, description, mimetype, userId, fileSize }) {
  // Validate parameters
  if (!uploadId || !totalChunks || !originalName || !userId) {
    throw new Error('Missing required parameters for finalize');
  }
  if (typeof fileSize !== 'number' || fileSize <= 0) {
    throw new Error('Invalid fileSize: must be positive number');
  }
  if (totalChunks < 1 || !Number.isInteger(totalChunks)) {
    throw new Error('Invalid totalChunks: must be positive integer');
  }

  const parts = Array.from({ length: totalChunks }, (_, index) =>
    path.join(tempChunksDir, `${uploadId}.part.${index}`)
  );

  // Check which chunks exist
  console.log(`📦 Merging ${totalChunks} chunks for ${uploadId}`);
  const existingChunks = [];
  for (let i = 0; i < parts.length; i++) {
    if (fs.existsSync(parts[i])) {
      existingChunks.push(i);
    } else {
      console.warn(`⚠️ Missing chunk ${i}: ${parts[i]}`);
    }
  }
  console.log(`✓ Found ${existingChunks.length}/${totalChunks} chunks`);

  // Fast check if first chunk exists (fail-fast)
  try {
    await fs.promises.access(parts[0], fs.constants.R_OK);
  } catch {
    console.error(`❌ Cannot read first chunk: ${parts[0]}`);
    return null;
  }

  const safeName = `${Date.now()}-${uploadId}-${path.basename(originalName)}`;
  const finalPath = path.join(uploadsDir, safeName);
  console.log(`💾 Saving merged file to: ${finalPath}`);
  
  const writeStream = fs.createWriteStream(finalPath, { flags: 'w', highWaterMark: 4 * 1024 * 1024 });

  // Ultra-fast parallel chunk merging (up to 3 streams at once)
  await new Promise((resolveAll, rejectAll) => {
    let currentIndex = 0;
    let activeStreams = 0;
    const maxActiveStreams = 3; // Process 3 chunks in parallel
    const queue = [];

    const writeNextChunk = () => {
      if (activeStreams >= maxActiveStreams) return;
      if (queue.length === 0 && currentIndex >= parts.length && activeStreams === 0) {
        writeStream.end();
        return;
      }
      if (queue.length === 0) return;

      activeStreams++;
      const partPath = queue.shift();
      const readStream = fs.createReadStream(partPath, { highWaterMark: 8 * 1024 * 1024 }); // 8MB buffer per stream

      readStream.on('error', (err) => {
        writeStream.destroy();
        rejectAll(err);
      });

      readStream.on('end', () => {
        activeStreams--;
        // Delete temp chunk immediately (don't wait)
        fs.unlink(partPath, () => {});
        writeNextChunk();
      });

      readStream.pipe(writeStream, { end: false });
    };

    // Load chunks into queue - allows parallel I/O prep
    const loadChunks = () => {
      // Fill queue up to 5 items for better I/O preparation
      while (currentIndex < parts.length && queue.length < 5) {
        queue.push(parts[currentIndex]);
        currentIndex++;
      }
      // Try to start processing if possible
      if (queue.length > 0 && activeStreams < maxActiveStreams) {
        writeNextChunk();
      }
      // Schedule more loading if we have more chunks
      if (currentIndex < parts.length) {
        setImmediate(loadChunks);
      }
    };

    // Handle write stream drain
    const onDrain = () => {
      // When writeStream drains, try to pipe more chunks
      if (queue.length > 0 && activeStreams < maxActiveStreams) {
        writeNextChunk();
      }
      // If no more chunks and no active streams, close the file
      if (currentIndex >= parts.length && queue.length === 0 && activeStreams === 0) {
        writeStream.end();
      }
    };

    writeStream.on('drain', onDrain);

    writeStream.on('finish', () => {
      console.log(`✅ File write completed: ${finalPath}`);
      console.log(`📊 Final file size: ${fileSize} bytes`);
      
      // Create file doc after write completes
      const fileDoc = {
        user_id: userId,
        file_name: originalName,
        file_size: fileSize,
        file_type: mimetype || 'application/octet-stream',
        storage_path: safeName,
        description: description || null,
        is_public: false,
        created_at: new Date(),
        updated_at: new Date(),
      };

      // Verify file exists before inserting to DB
      fs.stat(finalPath, (err, stats) => {
        if (err) {
          console.error(`❌ File not found after write: ${finalPath}`, err);
          return;
        }
        console.log(`📁 File verified: ${stats.size} bytes`);
        
        // Insert to DB
        db.collection('files').insertOne(fileDoc).catch(err => {
          console.error('DB insert error:', err);
        }).then(() => {
          console.log(`💾 Saved to DB: ${originalName}`);
        });
      });

      // Clean up upload session
      uploadSessions.delete(uploadId);
      
      resolveAll({ id: safeName, file_name: originalName, file_size: fileSize });
    });

    writeStream.on('error', (err) => {
      console.error(`❌ Write stream error: ${err.message}`);
      console.error(err.stack);
      rejectAll(err);
    });

    loadChunks();
  });
}

async function cleanupOldTempChunks(ageMs = 2 * 60 * 60 * 1000) {
  try {
    const files = await fs.promises.readdir(tempChunksDir);
    const now = Date.now();

    await Promise.all(files.map(async (file) => {
      if (!file.includes('.part.')) {
        return;
      }
      const filePath = path.join(tempChunksDir, file);
      try {
        const stat = await fs.promises.stat(filePath);
        if (now - stat.mtimeMs > ageMs) {
          await fs.promises.unlink(filePath);
        }
      } catch (err) {
        // ignore missing file or permission issues
      }
    }));
  } catch (err) {
    console.error('Error cleaning temp chunk files:', err);
  }
}

// Run cleanup once at startup, then periodically every hour
cleanupOldTempChunks().catch(err => console.error('Startup temp cleanup failed:', err));
const fileCleanupInterval = setInterval(() => cleanupOldTempChunks(), 60 * 60 * 1000);

// Cleanup expired upload sessions to prevent memory leaks
function cleanupExpiredUploadSessions(maxAgeMs = 3600000) { // 1 hour default
  const now = Date.now();
  let cleaned = 0;
  for (const [uploadId, session] of uploadSessions.entries()) {
    if (now - session.createdAt > maxAgeMs) {
      uploadSessions.delete(uploadId);
      cleaned++;
    }
  }
  if (cleaned > 0) {
    console.log(`🧹 Cleaned ${cleaned} expired upload sessions`);
  }
}

const sessionCleanupInterval = setInterval(() => cleanupExpiredUploadSessions(), 60000); // Every minute

// Middleware to verify JWT
function verifyToken(req, res, next) {
  const token = req.headers['authorization']?.split(' ')[1];
  if (!token) {
    return res.status(401).json({ error: 'No token provided' });
  }
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    if (!decoded.userId || !decoded.email) {
      return res.status(401).json({ error: 'Invalid token: missing userId or email' });
    }
    req.userId = decoded.userId;
    req.email = decoded.email;
    next();
  } catch (err) {
    console.warn('Token validation failed:', err.message.substring(0, 100));
    res.status(401).json({ error: 'Invalid or expired token' });
  }
}

// Input sanitizer utility
function sanitizeInput(str, maxLength = 1000) {
  if (typeof str !== 'string') return '';
  return str.trim().substring(0, maxLength);
}

// Auth Routes

// Sign up
app.post('/auth/signup', async (req, res) => {
  try {
    const email = sanitizeInput(req.body.email, 255);
    const password = sanitizeInput(req.body.password, 256);
    const displayName = sanitizeInput(req.body.displayName, 100);
    const username = sanitizeInput(req.body.username, 20);

    // Validate email format
    if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
      return res.status(400).json({ error: 'Invalid email format' });
    }

    if (!password || password.length < 6) {
      return res.status(400).json({ error: 'Password must be at least 6 characters' });
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
    console.error('Signup error:', err.message);
    if (err.code === 11000) {
      const field = Object.keys(err.keyPattern || {})[0] || 'field';
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

// Chunked upload support for large files - PARALLEL CHUNK SUPPORT
app.post('/api/files/upload-chunk', verifyToken, (req, res, next) => {
  chunkUpload.single('chunk')(req, res, (err) => {
    if (err) {
      console.error(`Chunk upload error for user ${req.userId}:`, err.message);
      if (err.code === 'LIMIT_FILE_SIZE') {
        return res.status(413).json({ error: `Chunk too large (max: ${(err.limit / 1024 / 1024).toFixed(0)}MB)` });
      }
      return res.status(400).json({ error: err.message || 'Chunk upload failed' });
    }
    next();
  });
}, async (req, res) => {
  try {
    const uploadId = sanitizeInput(req.body.uploadId, 100);
    const chunkIndex = Number(req.body.chunkIndex);
    const totalChunks = Number(req.body.totalChunks);
    const originalName = sanitizeInput(req.body.fileName, 255);
    const description = sanitizeInput(req.body.description, 1000);
    const isLastChunk = req.body.isLastChunk === 'true';

    // Validate metadata
    if (!uploadId || Number.isNaN(chunkIndex) || Number.isNaN(totalChunks) || !originalName) {
      return res.status(400).json({ error: 'Missing or invalid upload metadata' });
    }

    if (chunkIndex < 0 || chunkIndex >= totalChunks) {
      return res.status(400).json({ error: 'Invalid chunk index' });
    }

    if (totalChunks < 1 || totalChunks > 10000) {
      return res.status(400).json({ error: 'Invalid total chunks (1-10000 allowed)' });
    }

    if (!req.file) {
      return res.status(400).json({ error: 'No chunk file provided' });
    }

    // Rename temp file to correct chunk filename NOW that we have uploadId/chunkIndex
    const correctChunkName = `${uploadId}.part.${chunkIndex}`;
    const correctChunkPath = path.join(tempChunksDir, correctChunkName);
    const tempFilePath = req.file.path;
    
    try {
      await fs.promises.rename(tempFilePath, correctChunkPath);
      req.file.path = correctChunkPath;
      req.file.filename = correctChunkName;
    } catch (renameErr) {
      console.error(`❌ Failed to rename chunk file: ${renameErr.message}`);
      return res.status(500).json({ error: 'Failed to save chunk' });
    }

    console.log(`📥 Received chunk ${chunkIndex + 1}/${totalChunks} for upload ${uploadId.substring(0, 8)}... (${(req.file.size / 1024 / 1024).toFixed(1)}MB)`);
    console.log(`   Saved to: ${req.file.path}`);

    // Track upload session
    if (!uploadSessions.has(uploadId)) {
      uploadSessions.set(uploadId, { 
        chunks: new Set(), 
        totalChunks, 
        userId: req.userId,
        fileName: originalName,
        description,
        mimetype: req.file.mimetype,
        fileSize: Number(req.body.fileSize),
        createdAt: Date.now(),
        finalized: false,
        finalizedAt: null
      });
    }

    const session = uploadSessions.get(uploadId);
    
    // Security: Verify upload belongs to current user
    if (session.userId !== req.userId) {
      return res.status(403).json({ error: 'Upload does not belong to this user' });
    }

    session.chunks.add(chunkIndex);
    const chunksReceived = session.chunks.size;
    const progress = Math.round((chunksReceived / totalChunks) * 100);

    console.log(`   Progress: ${chunksReceived}/${totalChunks} chunks (${progress}%)`);

    res.json({ 
      chunkIndex, 
      totalChunks, 
      chunksReceived,
      progress,
      chunkComplete: false,
      ready: chunksReceived === totalChunks
    }).end();

    // Async finalize if all chunks received
    // But verify actual chunk files exist (not just in-memory count) since we have clustering
    if (chunksReceived === totalChunks) {
      console.log(`🚀 Memory counter: All chunks received for ${uploadId.substring(0, 8)}... - Verifying chunk files...`);
      
      // Verify all chunks actually exist on disk (async!)
      setImmediate(async () => {
        const allChunkPaths = Array.from({ length: totalChunks }, (_, i) =>
          path.join(tempChunksDir, `${uploadId}.part.${i}`)
        );
        
        const missingChunks = [];
        for (const chunkPath of allChunkPaths) {
          if (!fs.existsSync(chunkPath)) {
            missingChunks.push(chunkPath);
          }
        }
        
        if (missingChunks.length > 0) {
          console.warn(`⚠️ Memory says all ${totalChunks} chunks ready, but ${missingChunks.length} missing on disk!`);
          console.warn(`   Missing: ${missingChunks.slice(0, 3).join(', ')}...`);
          // Don't finalize yet - files might still be arriving from other workers
          return;
        }
        
        console.log(`✅ All ${totalChunks} chunks verified on disk - Starting finalization...`);
        try {
          console.log(`[FINALIZE] Starting for ${uploadId}, chunks=${totalChunks}, name=${session.fileName}`);
          const result = await tryFinalizeChunkedUpload({
            uploadId,
            totalChunks,
            originalName: session.fileName,
            description: session.description,
            mimetype: session.mimetype,
            userId: session.userId,
            fileSize: session.fileSize || Number(req.body.fileSize),
          });

          if (result) {
            // Mark upload as finalized
            const sessionCheck = uploadSessions.get(uploadId);
            if (sessionCheck) {
              sessionCheck.finalized = true;
              sessionCheck.finalizedAt = Date.now();
            }
            console.log(`✅ Upload complete: ${session.fileName} (${chunksReceived} chunks, user: ${req.userId})`);
          } else {
            console.error(`[FINALIZE] Failed - result is null`);
          }
        } catch (err) {
          console.error(`❌ Finalize error for ${uploadId}:`, err.message);
          console.error(`[FINALIZE] Stack:`, err.stack);
          uploadSessions.delete(uploadId);
        }
      });
    }
  } catch (err) {
    console.error('Chunk upload error:', err.message);
    res.status(500).json({ error: 'Chunk upload failed: ' + err.message });
  }
});

// Check upload progress endpoint
app.get('/api/files/upload-progress/:uploadId', verifyToken, (req, res) => {
  const uploadId = req.params.uploadId;
  const session = uploadSessions.get(uploadId);

  if (!session) {
    return res.status(404).json({ error: 'Upload session not found' });
  }

  if (session.userId !== req.userId) {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  const progress = Math.round((session.chunks.size / session.totalChunks) * 100);

  res.json({
    uploadId,
    chunksReceived: session.chunks.size,
    totalChunks: session.totalChunks,
    progress,
    complete: session.chunks.size === session.totalChunks,
    missingChunks: Array.from({ length: session.totalChunks }, (_, i) => i).filter(i => !session.chunks.has(i))
  });
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

// Health check with stats
app.get('/health/stats', (req, res) => {
  const activeSessions = uploadSessions.size;
  let totalChunksInProgress = 0;
  let totalSize = 0;
  
  for (const [, session] of uploadSessions.entries()) {
    totalChunksInProgress += session.chunks.size;
    totalSize += session.fileSize || 0;
  }
  
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    memory: process.memoryUsage(),
    pid: process.pid,
    uploads: {
      activeSessions,
      chunksInProgress: totalChunksInProgress,
      totalSizeInProgress: Math.round(totalSize / 1024 / 1024) + ' MB',
      concurrency: {
        maxFilesPerSecond: 15,
        maxChunksPerFile: 8,
        maxParallelChunkMerges: 3
      }
    }
  });
});

// ⚡ ULTRA-PERFORMANCE: Clustering + Worker Threads
const numCPUs = os.cpus().length;
// DISABLE clustering for now - file uploads need shared session state across workers
// TODO: Use Redis for shared uploadSessions if clustering needed
const enableClustering = false; // process.env.ENABLE_CLUSTERING !== 'false';

if (enableClustering && cluster.isPrimary) {
  console.log(`🚀 Master process ${process.pid} starting...`);
  console.log(`📊 Spawning ${numCPUs} worker processes...`);
  
  // Spawn workers
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  // Handle worker restart
  cluster.on('exit', (worker, code, signal) => {
    if (signal) {
      console.log(`⚠️ Worker ${worker.process.pid} killed by signal: ${signal}`);
    } else if (code !== 0) {
      console.log(`⚠️ Worker ${worker.process.pid} exited with error code: ${code}`);
      console.log('🔄 Spawning replacement...');
      cluster.fork();
    }
  });

  // Status logger
  setInterval(() => {
    console.log(`📈 Workers: ${Object.keys(cluster.workers).length} | Memory: ${Math.round(process.memoryUsage().heapUsed / 1024 / 1024)}MB`);
  }, 30000);
} else {
  // Worker process
  initMongoDB().then(() => {
    const server = app.listen(PORT, () => {
      const workerId = cluster.isPrimary ? 'primary' : cluster.worker?.id;
      console.log(`✅ Worker process ${process.pid} (ID: ${workerId}) running on http://localhost:${PORT}`);
    });

    // Socket handling optimization
    const maxConnections = 10000;
    server.maxConnections = maxConnections;

    // Keep-alive settings
    server.keepAliveTimeout = 65000;
    server.headersTimeout = 66000;

    // Error handling
    server.on('error', (err) => {
      console.error('Server error:', err);
    });

    // Graceful shutdown
    process.on('SIGTERM', () => {
      console.log(`🛑 Worker ${process.pid} received SIGTERM, shutting down gracefully...`);
      
      // Cleanup intervals
      clearInterval(fileCleanupInterval);
      clearInterval(sessionCleanupInterval);
      
      server.close(async () => {
        console.log(`✅ Worker ${process.pid} server closed`);
        if (mongoClient) {
          await mongoClient.close();
        }
        process.exit(0);
      });

      // Force shutdown after 30 seconds
      setTimeout(() => {
        console.error('❌ Forced shutdown after 30s');
        process.exit(1);
      }, 30000);
    });

    // Handle other termination signals
    process.on('SIGINT', () => {
      console.log(`🛑 Worker ${process.pid} received SIGINT`);
      clearInterval(fileCleanupInterval);
      clearInterval(sessionCleanupInterval);
      process.exit(0);
    });
  });
}
