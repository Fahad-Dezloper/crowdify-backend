import { WebSocketServer, WebSocket } from "ws";
import * as http from "http";
import * as dotenv from "dotenv";
import { createClient } from "redis";
// For pub/sub between server instances
import Redis from "ioredis";

dotenv.config();

const PORT = process.env.WS_PORT || 4000;
const SERVER_ID = process.env.SERVER_ID || `server-${Math.floor(Math.random() * 10000)}`;
const REDIS_URI = process.env.REDIS_URI || "redis://localhost:6379";

// Create HTTP server and WebSocket server
const server = http.createServer();
const wss = new WebSocketServer({ server });

// Redis client for data operations
const redisClient = createClient({
  socket: {
    host: "localhost",
    port: 6379,
  },
});

// Dedicated pub/sub clients
const subscriber = new Redis(REDIS_URI);
const publisher = new Redis(REDIS_URI);

// Connect to Redis
async function connectToRedis() {
  try {
    await redisClient.connect();
    console.log("Connected to Redis for data operations");
    
    // Subscribe to room events channel
    subscriber.subscribe("room-events", (err) => {
      if (err) {
        console.error("Failed to subscribe to room events:", err);
        return;
      }
      console.log("Subscribed to room events channel");
    });
    
    // Set up message handler for subscriber
    subscriber.on("message", (channel, message) => {
      handleRedisMessage(channel, message);
    });
    
  } catch (error) {
    console.error("Redis connection error:", error);
    // Retry connection after delay
    setTimeout(connectToRedis, 5000);
  }
}

connectToRedis();

// Maps to track connections by room (in-memory for this server only)
const connectionsByRoom = new Map<string, Set<WebSocket>>();
const connectionToRoom = new Map<WebSocket, string>();
const userIdsByRoom = new Map<string, Set<string>>();
const connectionToUserId = new Map<WebSocket, string>();

// Handle messages from Redis pub/sub
function handleRedisMessage(channel: string, message: string) {
  if (channel !== "room-events") return;
  
  try {
    const eventData = JSON.parse(message);
    const { roomId, event, data, source } = eventData;
    
    // Ignore events from this server to avoid duplicates
    if (source === SERVER_ID) return;
    
    // Get connections for this room on this server
    const roomConnections = connectionsByRoom.get(roomId);
    if (!roomConnections) return;
    
    // Broadcast to all connections in the room on this server
    roomConnections.forEach(conn => {
      if (conn.readyState === WebSocket.OPEN) {
        conn.send(JSON.stringify({ type: event, ...data }));
      }
    });
    
    // Update local user count if userJoined or userLeft event from another server
    if (event === "userJoined" || event === "userLeft") {
      updateLocalUserCount(roomId, data.userId, event === "userJoined");
    }
    
  } catch (error) {
    console.error("Error handling Redis message:", error);
  }
}

// Update local tracking of users when receiving events from other servers
function updateLocalUserCount(roomId: string, userId: string, isJoining: boolean) {
  if (!userIdsByRoom.has(roomId)) {
    userIdsByRoom.set(roomId, new Set());
  }
  
  const roomUsers = userIdsByRoom.get(roomId)!;
  
  if (isJoining) {
    roomUsers.add(userId);
  } else {
    roomUsers.delete(userId);
  }
}

// Broadcast to all clients in a room across all server instances
async function broadcastToRoom(roomId: string, event: string, data: any) {
  try {
    // First, broadcast to clients on this server
    const roomConnections = connectionsByRoom.get(roomId);
    if (roomConnections) {
      roomConnections.forEach(conn => {
        if (conn.readyState === WebSocket.OPEN) {
          conn.send(JSON.stringify({ type: event, ...data }));
        }
      });
    }
    
    // Then publish the event to Redis for other servers
    await publisher.publish("room-events", JSON.stringify({
      roomId,
      event,
      data,
      source: SERVER_ID,
      timestamp: Date.now()
    }));
    
  } catch (error) {
    console.error(`Error broadcasting to room ${roomId}:`, error);
  }
}

// Track active rooms and set expiration
async function markRoomActive(roomId: string) {
  try {
    const key = `room:${roomId}:active`;
    await redisClient.set(key, "true");
    
    // Set room to expire after 24 hours of inactivity
    await redisClient.expire(key, 24 * 60 * 60); // 24 hours in seconds
  } catch (error) {
    console.error(`Error marking room ${roomId} as active:`, error);
  }
}

// Cleanup expired rooms (called by scheduled job outside this process)
async function cleanupRoom(roomId: string) {
  try {
    console.log(`Cleaning up inactive room: ${roomId}`);
    
    // Get all keys related to this room
    const roomKeys = await redisClient.keys(`*${roomId}*`);
    
    if (roomKeys.length > 0) {
      await redisClient.del(roomKeys);
      console.log(`Deleted ${roomKeys.length} keys for room ${roomId}`);
    }
    
  } catch (error) {
    console.error(`Error cleaning up room ${roomId}:`, error);
  }
}

// Get total user count for a room across all server instances
async function getRoomUserCount(roomId: string): Promise<number> {
  try {
    const onlineUsers = await redisClient.sMembers(`onlineUsers:${roomId}`);
    return onlineUsers.length;
  } catch (error) {
    console.error(`Error getting user count for room ${roomId}:`, error);
    return 0;
  }
}

// Add user to room in Redis
async function addUserToRoom(roomId: string, userId: string) {
  try {
    await redisClient.sAdd(`onlineUsers:${roomId}`, userId);
    const count = await getRoomUserCount(roomId);
    return count;
  } catch (error) {
    console.error(`Error adding user ${userId} to room ${roomId}:`, error);
    return 0;
  }
}

// Remove user from room in Redis
async function removeUserFromRoom(roomId: string, userId: string) {
  try {
    await redisClient.sRem(`onlineUsers:${roomId}`, userId);
    const count = await getRoomUserCount(roomId);
    return count;
  } catch (error) {
    console.error(`Error removing user ${userId} from room ${roomId}:`, error);
    return 0;
  }
}

// Handle a new connection joining a room
async function handleJoinRoom(ws: WebSocket, roomId: string, userId: string) {
  try {
    // Update room activity timestamp
    await markRoomActive(roomId);
    
    // Add connection to room tracking
    if (!connectionsByRoom.has(roomId)) {
      connectionsByRoom.set(roomId, new Set());
    }
    connectionsByRoom.get(roomId)?.add(ws);
    connectionToRoom.set(ws, roomId);
    
    // Track user ID
    connectionToUserId.set(ws, userId);
    
    // Add user to room's user set
    if (!userIdsByRoom.has(roomId)) {
      userIdsByRoom.set(roomId, new Set());
    }
    userIdsByRoom.get(roomId)?.add(userId);
    
    // Add user to room in Redis and get updated count
    const userCount = await addUserToRoom(roomId, userId);
    
    console.log(`User joined room: ${userId} in ${roomId} (server: ${SERVER_ID}), total users: ${userCount}`);
    
    // Send room state to the new connection
    await sendRoomState(ws, roomId, userId);
    
    // Broadcast user joined event with updated count to all users in the room
    await broadcastToRoom(roomId, "userJoined", { 
      userId, 
      userCount
    });
    
  } catch (error) {
    console.error(`Error handling join for room ${roomId}:`, error);
    ws.send(JSON.stringify({ 
      type: "error", 
      message: "Failed to join room. Please try again." 
    }));
  }
}

// Send the current room state to a connection
async function sendRoomState(ws: WebSocket, roomId: string, userId: string) {
  try {
    // Get user count
    const userCount = await getRoomUserCount(roomId);
    ws.send(JSON.stringify({ 
      type: "userCount", 
      count: userCount 
    }));
    
    // Get chat status
    const chatStatus = await redisClient.get(`chatStatus:${roomId}`);
    ws.send(JSON.stringify({ 
      type: "chatStatus", 
      paused: chatStatus === "paused" 
    }));
    
    // Get song add status
    const songAddStatus = await redisClient.get(`allowSongAdd:${roomId}`);
    ws.send(JSON.stringify({ 
      type: "allowSongAdd", 
      paused: songAddStatus === "paused" 
    }));
    
    // Get chat history
    const messages = await redisClient.lRange(`chat:${roomId}`, 0, -1);
    messages.reverse().forEach((msg) => {
      ws.send(JSON.stringify({ 
        type: "message", 
        ...JSON.parse(msg) 
      }));
    });
    
    // Get song queue
    const songs = await redisClient.lRange(`queue:${roomId}`, 0, -1);
    let parsedSongs = songs.map(song => JSON.parse(song));

    // Check if user has liked each song
    for (let song of parsedSongs) {
      const userVoteKey = `vote:${roomId}:${song.streamId}:${userId}`;
      const userVote = await redisClient.get(userVoteKey);
      song.hasLiked = userVote === "upvote";
    }

    ws.send(JSON.stringify({ 
      type: "songQueue", 
      queue: parsedSongs 
    }));

    // Get now playing song
    const nowPlayingSong = await redisClient.get(`nowPlaying:${roomId}`);
    if (nowPlayingSong) {
      ws.send(JSON.stringify({ 
        type: "nowPlaying", 
        song: JSON.parse(nowPlayingSong) 
      }));
    }
    
  } catch (error) {
    console.error(`Error sending room state for ${roomId}:`, error);
  }
}

// WebSocket connection handling
wss.on("connection", (ws) => {
  console.log(`New WebSocket connection on server ${SERVER_ID}`);
  
  ws.on("message", async (message) => {
    try {
      const data = JSON.parse(message.toString());
      const roomId = data.roomId || connectionToRoom.get(ws);
      const userId = data.userId || connectionToUserId.get(ws);
      
      if (!roomId && !userId && data.type !== "join") {
        console.error("No room ID for message:", data);
        return;
      }
      
      // Handle different message types
      switch (data.type) {
        case "join":
          console.log("you joined baby", data.userId);
          await handleJoinRoom(ws, data.roomId, data.userId);
          break;
          
        case "message":
          // Store message in Redis
          const messageData = JSON.stringify({ text: data.text, sender: data.sender });
          await redisClient.lPush(`chat:${roomId}`, messageData);
          await redisClient.lTrim(`chat:${roomId}`, 0, 49);
          
          // Check if chat is paused
          const chatStatus = await redisClient.get(`chatStatus:${roomId}`);
          if (chatStatus === "paused") {
            ws.send(JSON.stringify({ 
              type: "chatError", 
              message: "Chat is currently paused by the room admin"
            }));
            return;
          }
          
          // Broadcast to all users in the room across all servers
          await broadcastToRoom(roomId, "message", { 
            text: data.text, 
            sender: data.sender 
          });
          
          // Update room activity
          await markRoomActive(roomId);
          break;
          
        case "addSong":
          console.log("event happened for add song");
          console.log(data);
          
          // Add song to queue
          await redisClient.rPush(`queue:${roomId}`, JSON.stringify(data.song));
          
          // Get updated queue
          const songs = await redisClient.lRange(`queue:${roomId}`, 0, -1);
          const parsedSongs = songs.map((song) => JSON.parse(song));
          
          // Broadcast updates
          await broadcastToRoom(roomId, "addSong", { song: data.song });
          await broadcastToRoom(roomId, "songQueue", { queue: parsedSongs });
          
          // Update room activity
          await markRoomActive(roomId);
          break;
          
        case "voteUpdate":
          if (!data.songId || !data.voteType || !data.userId) {
            console.error("Invalid vote data:", data);
            return;
          }
          
          const userVoteKey = `vote:${roomId}:${data.songId}:${data.userId}`;
          const songQueueKey = `queue:${roomId}`;
          
          // Process vote
          const existingVote = await redisClient.get(userVoteKey);
          const songsData = await redisClient.lRange(songQueueKey, 0, -1);
          
          const uniqueSongsMap = new Map();
          songsData.forEach(songString => {
            const song = JSON.parse(songString);
            uniqueSongsMap.set(song.streamId, song);
          });
          
          let parsedQueue = Array.from(uniqueSongsMap.values());
          
          let updatedQueue = parsedQueue.map(song => {
            if (song.streamId === data.songId) {
              let newUpvoteCount = song.upvoteCount || 0;
              
              if (existingVote === data.voteType) {
                newUpvoteCount += (data.voteType === "upvote") ? -1 : 0;
                redisClient.del(userVoteKey);
              } 
              else if (existingVote && existingVote !== data.voteType) {
                if (data.voteType === "upvote") {
                  newUpvoteCount += 1;
                }
                else {
                  newUpvoteCount = Math.max(newUpvoteCount - 1, 0);
                }
                redisClient.set(userVoteKey, data.voteType);
              } 
              else {
                newUpvoteCount += (data.voteType === "upvote") ? 1 : 0;
                redisClient.set(userVoteKey, data.voteType);
              }
              
              return { ...song, upvoteCount: newUpvoteCount };
            }
            return song;
          });
          
          // Update queue in Redis
          const multi = redisClient.multi();
          multi.del(songQueueKey);
          
          for (const song of updatedQueue) {
            multi.rPush(songQueueKey, JSON.stringify(song));
          }
          
          await multi.exec();
          
          // Broadcast updates
          await broadcastToRoom(roomId, "voteUpdate", { queue: updatedQueue });
          
          // Update room activity
          await markRoomActive(roomId);
          break;
          
        case "nextSong":
          const songQueue = `queue:${roomId}`;
          const historyKey = `history:${roomId}`;
          const nowPlayingKey = `nowPlaying:${roomId}`;
          
          // Move current song to history
          const currentSongStr = await redisClient.get(nowPlayingKey);
          if (currentSongStr) {
            await redisClient.lPush(historyKey, currentSongStr);
            await redisClient.lTrim(historyKey, 0, 4);
          }
          
          // Get song queue
          const queueSongs = await redisClient.lRange(songQueue, 0, -1);
          const queueParsedSongs = queueSongs.map(song => JSON.parse(song));
          
          if (queueParsedSongs.length > 0) {
            // Find most upvoted song
            const mostUpvotedSong = queueParsedSongs.reduce((prev, curr) => 
              (prev?.upvoteCount || 0) > (curr?.upvoteCount || 0) ? prev : curr, queueParsedSongs[0]);
            
            // Set as now playing
            await redisClient.set(nowPlayingKey, JSON.stringify(mostUpvotedSong));
            
            // Remove from queue
            const updatedQueue = queueParsedSongs.filter(song => 
              song.streamId !== mostUpvotedSong.streamId);
            
            // Update queue in Redis
            const queueMulti = redisClient.multi();
            queueMulti.del(songQueue);
            
            for (const song of updatedQueue) {
              queueMulti.rPush(songQueue, JSON.stringify(song));
            }
            
            await queueMulti.exec();
            
            // Broadcast updates
            await broadcastToRoom(roomId, "nowPlaying", { song: mostUpvotedSong });
            await broadcastToRoom(roomId, "songQueue", { queue: updatedQueue });
          }
          
          // Update room activity
          await markRoomActive(roomId);
          break;
          
        case "chatpause":
          // Toggle chat pause state
          const room = await redisClient.get(`chatStatus:${roomId}`);
          const newChatStatus = room === "paused" ? "active" : "paused";
          
          await redisClient.set(`chatStatus:${roomId}`, newChatStatus);
          
          // Broadcast update
          await broadcastToRoom(roomId, "chatStatus", { paused: newChatStatus === "paused" });
          
          // Update room activity
          await markRoomActive(roomId);
          break;
          
        case "allowSongAdd":
          // Toggle song add state
          const songAddState = await redisClient.get(`allowSongAdd:${roomId}`);
          const newSongAddState = songAddState === "paused" ? "active" : "paused";
          
          await redisClient.set(`allowSongAdd:${roomId}`, newSongAddState);
          
          // Broadcast update
          await broadcastToRoom(roomId, "allowSongAdd", { paused: newSongAddState === "paused" });
          
          // Update room activity
          await markRoomActive(roomId);
          break;
          
        default:
          console.log(`Unknown message type: ${data.type}`);
      }
      
    } catch (error) {
      console.error("Error processing message:", error);
    }
  });
  
  // Handle WebSocket disconnection
  ws.on("close", async () => {
    const roomId = connectionToRoom.get(ws);
    const userId = connectionToUserId.get(ws);
    
    if (roomId && userId) {
      // Remove connection from tracking
      connectionToRoom.delete(ws);
      connectionToUserId.delete(ws);
      
      const roomConnections = connectionsByRoom.get(roomId);
      if (roomConnections) {
        roomConnections.delete(ws);
        
        if (roomConnections.size === 0) {
          connectionsByRoom.delete(roomId);
        }
      }
      
      // Check if this is the last connection for this user in this room
      let isLastConnectionForUser = true;
      connectionsByRoom.get(roomId)?.forEach(conn => {
        if (connectionToUserId.get(conn) === userId) {
          isLastConnectionForUser = false;
        }
      });
      
      // If it's the last connection, remove the user from the room
      if (isLastConnectionForUser) {
        // Remove from local tracking
        userIdsByRoom.get(roomId)?.delete(userId);
        
        // Remove from Redis and get updated count
        const userCount = await removeUserFromRoom(roomId, userId);
        
        console.log(`User left room: ${userId} in ${roomId} (server: ${SERVER_ID}), remaining users: ${userCount}`);
        
        // Broadcast user left with updated count
        await broadcastToRoom(roomId, "userLeft", {
          userId,
          userCount
        });
      }
    }
  });
});

// Set up health check endpoint for load balancers
server.on("request", (req, res) => {
  if (req.url === "/health") {
    res.writeHead(200);
    res.end(`OK - Server ${SERVER_ID} is healthy`);
  }
});

// Graceful shutdown
process.on("SIGINT", gracefulShutdown);
process.on("SIGTERM", gracefulShutdown);

async function gracefulShutdown() {
  console.log(`Shutting down server ${SERVER_ID}...`);
  
  try {
    // Close Redis connections
    await redisClient.quit();
    await subscriber.quit();
    await publisher.quit();
    
    // Close WebSocket server
    wss.close();
    
    // Close HTTP server
    server.close();
    
    console.log(`Server ${SERVER_ID} shut down gracefully`);
    process.exit(0);
  } catch (error) {
    console.error("Error during shutdown:", error);
    process.exit(1);
  }
}

server.listen(PORT, () => console.log(`WebSocket server ${SERVER_ID} running on port ${PORT}`));