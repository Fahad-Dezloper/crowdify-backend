import { WebSocketServer, WebSocket } from "ws";
import * as http from "http";
import * as dotenv from "dotenv";
import { createClient } from "redis";
import Redis from "ioredis";

dotenv.config();

const PORT = process.env.WS_PORT || 4000;
const SERVER_ID = process.env.SERVER_ID || `server-${Math.floor(Math.random() * 10000)}`;
const REDIS_URI = process.env.REDIS_URL || "redis://localhost:6379";

const server = http.createServer();
const wss = new WebSocketServer({ server });

// const redisClient = createClient({
//   socket: {
//     host: "localhost",
//     port: 6379,
//   },
// });

// Redis configuration for Railway
const redisClient = createClient({
  url: process.env.REDIS_URL || "redis://localhost:6379"
});



const subscriber = new Redis(REDIS_URI);
const publisher = new Redis(REDIS_URI);

async function connectToRedis() {
  try {
    await redisClient.connect();
    console.log("Connected to Redis for data operations");
    subscriber.subscribe("room-events", (err) => {
      if (err) {
        console.error("Failed to subscribe to room events:", err);
        return;
      }
      console.log("Subscribed to room events channel");
    });
    subscriber.on("message", (channel, message) => {
      handleRedisMessage(channel, message);
    });
  } catch (error) {
    console.error("Redis connection error:", error);
    setTimeout(connectToRedis, 5000);
  }
}

connectToRedis();

const connectionsByRoom = new Map<string, Set<WebSocket>>();
const connectionToRoom = new Map<WebSocket, string>();
const userIdsByRoom = new Map<string, Set<string>>();
const connectionToUserId = new Map<WebSocket, string>();

function handleRedisMessage(channel: string, message: string) {
  if (channel !== "room-events") return;
  try {
    const eventData = JSON.parse(message);
    const { roomId, event, data, source } = eventData;
    if (source === SERVER_ID) return;
    const roomConnections = connectionsByRoom.get(roomId);
    if (!roomConnections) return;
    roomConnections.forEach(conn => {
      if (conn.readyState === WebSocket.OPEN) {
        conn.send(JSON.stringify({ type: event, ...data }));
      }
    });
    if (event === "userJoined" || event === "userLeft") {
      updateLocalUserCount(roomId, data.userId, event === "userJoined");
    }
  } catch (error) {
    console.error("Error handling Redis message:", error);
  }
}

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

async function broadcastToRoom(roomId: string, event: string, data: any) {
  try {
    const roomConnections = connectionsByRoom.get(roomId);
    if (roomConnections) {
      roomConnections.forEach(conn => {
        if (conn.readyState === WebSocket.OPEN) {
          conn.send(JSON.stringify({ type: event, ...data }));
        }
      });
    }
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

async function markRoomActive(roomId: string) {
  try {
    const key = `room:${roomId}:active`;
    await redisClient.set(key, "true");
    await redisClient.expire(key, 24 * 60 * 60);
  } catch (error) {
    console.error(`Error marking room ${roomId} as active:`, error);
  }
}

async function cleanupRoom(roomId: string) {
  try {
    console.log(`Cleaning up inactive room: ${roomId}`);
    const roomKeys = await redisClient.keys(`*${roomId}*`);
    if (roomKeys.length > 0) {
      await redisClient.del(roomKeys);
      console.log(`Deleted ${roomKeys.length} keys for room ${roomId}`);
    }
  } catch (error) {
    console.error(`Error cleaning up room ${roomId}:`, error);
  }
}

async function getRoomUserCount(roomId: string): Promise<number> {
  try {
    const onlineUsers = await redisClient.sMembers(`onlineUsers:${roomId}`);
    return onlineUsers.length;
  } catch (error) {
    console.error(`Error getting user count for room ${roomId}:`, error);
    return 0;
  }
}

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

async function handleJoinRoom(ws: WebSocket, roomId: string, userId: string) {
  try {
    await markRoomActive(roomId);
    if (!connectionsByRoom.has(roomId)) {
      connectionsByRoom.set(roomId, new Set());
    }
    connectionsByRoom.get(roomId)?.add(ws);
    connectionToRoom.set(ws, roomId);
    connectionToUserId.set(ws, userId);
    if (!userIdsByRoom.has(roomId)) {
      userIdsByRoom.set(roomId, new Set());
    }
    userIdsByRoom.get(roomId)?.add(userId);
    const userCount = await addUserToRoom(roomId, userId);
    console.log(`User joined room: ${userId} in ${roomId} (server: ${SERVER_ID}), total users: ${userCount}`);
    await sendRoomState(ws, roomId, userId);
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

async function sendRoomState(ws: WebSocket, roomId: string, userId: string) {
  try {
    const userCount = await getRoomUserCount(roomId);
    ws.send(JSON.stringify({ 
      type: "userCount", 
      count: userCount 
    }));
    const chatStatus = await redisClient.get(`chatStatus:${roomId}`);
    ws.send(JSON.stringify({ 
      type: "chatStatus", 
      paused: chatStatus === "paused" 
    }));
    const songAddStatus = await redisClient.get(`allowSongAdd:${roomId}`);
    ws.send(JSON.stringify({ 
      type: "allowSongAdd", 
      paused: songAddStatus === "paused" 
    }));
    const messages = await redisClient.lRange(`chat:${roomId}`, 0, -1);
    messages.reverse().forEach((msg) => {
      ws.send(JSON.stringify({ 
        type: "message", 
        ...JSON.parse(msg) 
      }));
    });
    const songs = await redisClient.lRange(`queue:${roomId}`, 0, -1);
    let parsedSongs = songs.map(song => JSON.parse(song));
    for (let song of parsedSongs) {
      const userVoteKey = `vote:${roomId}:${song.streamId}:${userId}`;
      const userVote = await redisClient.get(userVoteKey);
      song.hasLiked = userVote === "upvote";
    }
    ws.send(JSON.stringify({ 
      type: "songQueue", 
      queue: parsedSongs 
    }));
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
      switch (data.type) {
        case "join":
          console.log("you joined baby", data.userId);
          await handleJoinRoom(ws, data.roomId, data.userId);
          break;
        case "message":
          const messageData = JSON.stringify({ text: data.text, sender: data.sender });
          await redisClient.lPush(`chat:${roomId}`, messageData);
          await redisClient.lTrim(`chat:${roomId}`, 0, 49);
          const chatStatus = await redisClient.get(`chatStatus:${roomId}`);
          if (chatStatus === "paused") {
            ws.send(JSON.stringify({ 
              type: "chatError", 
              message: "Chat is currently paused by the room admin"
            }));
            return;
          }
          await broadcastToRoom(roomId, "message", { 
            text: data.text, 
            sender: data.sender 
          });
          await markRoomActive(roomId);
          break;
        case "addSong":
          console.log("event happened for add song");
          console.log(data);
          await redisClient.rPush(`queue:${roomId}`, JSON.stringify(data.song));
          const songs = await redisClient.lRange(`queue:${roomId}`, 0, -1);
          const parsedSongs = songs.map((song) => JSON.parse(song));
          await broadcastToRoom(roomId, "addSong", { song: data.song });
          await broadcastToRoom(roomId, "songQueue", { queue: parsedSongs });
          await markRoomActive(roomId);
          break;
        case "voteUpdate":
          if (!data.songId || !data.voteType || !data.userId) {
            console.error("Invalid vote data:", data);
            return;
          }
          const userVoteKey = `vote:${roomId}:${data.songId}:${data.userId}`;
          const songQueueKey = `queue:${roomId}`;
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
          const multi = redisClient.multi();
          multi.del(songQueueKey);
          for (const song of updatedQueue) {
            multi.rPush(songQueueKey, JSON.stringify(song));
          }
          await multi.exec();
          await broadcastToRoom(roomId, "voteUpdate", { queue: updatedQueue });
          await markRoomActive(roomId);
          break;
        case "nextSong":
          const songQueue = `queue:${roomId}`;
          const historyKey = `history:${roomId}`;
          const nowPlayingKey = `nowPlaying:${roomId}`;
          const currentSongStr = await redisClient.get(nowPlayingKey);
          if (currentSongStr) {
            await redisClient.lPush(historyKey, currentSongStr);
            await redisClient.lTrim(historyKey, 0, 4);
          }
          const queueSongs = await redisClient.lRange(songQueue, 0, -1);
          const queueParsedSongs = queueSongs.map(song => JSON.parse(song));
          if (queueParsedSongs.length > 0) {
            const mostUpvotedSong = queueParsedSongs.reduce((prev, curr) => 
              (prev?.upvoteCount || 0) > (curr?.upvoteCount || 0) ? prev : curr, queueParsedSongs[0]);
            await redisClient.set(nowPlayingKey, JSON.stringify(mostUpvotedSong));
            const updatedQueue = queueParsedSongs.filter(song => 
              song.streamId !== mostUpvotedSong.streamId);
            const queueMulti = redisClient.multi();
            queueMulti.del(songQueue);
            for (const song of updatedQueue) {
              queueMulti.rPush(songQueue, JSON.stringify(song));
            }
            await queueMulti.exec();
            await broadcastToRoom(roomId, "nowPlaying", { song: mostUpvotedSong });
            await broadcastToRoom(roomId, "songQueue", { queue: updatedQueue });
          }
          await markRoomActive(roomId);
          break;
        case "chatpause":
          const room = await redisClient.get(`chatStatus:${roomId}`);
          const newChatStatus = room === "paused" ? "active" : "paused";
          await redisClient.set(`chatStatus:${roomId}`, newChatStatus);
          await broadcastToRoom(roomId, "chatStatus", { paused: newChatStatus === "paused" });
          await markRoomActive(roomId);
          break;
        case "allowSongAdd":
          const songAddState = await redisClient.get(`allowSongAdd:${roomId}`);
          const newSongAddState = songAddState === "paused" ? "active" : "paused";
          await redisClient.set(`allowSongAdd:${roomId}`, newSongAddState);
          await broadcastToRoom(roomId, "allowSongAdd", { paused: newSongAddState === "paused" });
          await markRoomActive(roomId);
          break;
        default:
          console.log(`Unknown message type: ${data.type}`);
      }
    } catch (error) {
      console.error("Error processing message:", error);
    }
  });
  ws.on("close", async () => {
    const roomId = connectionToRoom.get(ws);
    const userId = connectionToUserId.get(ws);
    if (roomId && userId) {
      connectionToRoom.delete(ws);
      connectionToUserId.delete(ws);
      const roomConnections = connectionsByRoom.get(roomId);
      if (roomConnections) {
        roomConnections.delete(ws);
        if (roomConnections.size === 0) {
          connectionsByRoom.delete(roomId);
        }
      }
      let isLastConnectionForUser = true;
      connectionsByRoom.get(roomId)?.forEach(conn => {
        if (connectionToUserId.get(conn) === userId) {
          isLastConnectionForUser = false;
        }
      });
      if (isLastConnectionForUser) {
        userIdsByRoom.get(roomId)?.delete(userId);
        const userCount = await removeUserFromRoom(roomId, userId);
        console.log(`User left room: ${userId} in ${roomId} (server: ${SERVER_ID}), remaining users: ${userCount}`);
        await broadcastToRoom(roomId, "userLeft", {
          userId,
          userCount
        });
      }
    }
  });
});

server.on("request", (req, res) => {
  if (req.url === "/health") {
    res.writeHead(200);
    res.end(`OK - Server ${SERVER_ID} is healthy`);
  }
});

process.on("SIGINT", gracefulShutdown);
process.on("SIGTERM", gracefulShutdown);

async function gracefulShutdown() {
  console.log(`Shutting down server ${SERVER_ID}...`);
  try {
    await redisClient.quit();
    await subscriber.quit();
    await publisher.quit();
    wss.close();
    server.close();
    console.log(`Server ${SERVER_ID} shut down gracefully`);
    process.exit(0);
  } catch (error) {
    console.error("Error during shutdown:", error);
    process.exit(1);
  }
}

server.listen(PORT, () => console.log(`WebSocket server ${SERVER_ID} running on port ${PORT}`));
