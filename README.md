# CROWDIFY Backend

## Setup Instructions

Follow these steps to set up the backend for CROWDIFY.

### Step 1: Clone the Frontend Repository
First, clone the frontend repository:
```sh
git clone https://github.com/Fahad-Dezloper/Crowdify
```

### Step 2: Fork and Clone the Backend Repository
Fork the repository and then clone it to your local machine:
```sh
git clone https://github.com/Fahad-Dezloper/crowdify-backend
```

### Step 3: Update Redis Configuration
Navigate to `index.ts` and modify the Redis configuration as follows:

1. Uncomment the following lines:
```ts
const redisClient = createClient({
    socket: {
        host: "localhost",
        port: 6379,
    },
});
```
2. Comment out the existing Railway Redis configuration:
```ts
// Redis configuration for Railway
// const redisClient = createClient({
//   url: process.env.REDIS_URL || "redis://localhost:6379"
// });
```

### Step 4: Run Redis in Docker
Run the following command to start a Redis container using Docker:
```sh
docker run --name redis-container -p 6379:6379 -d redis
```

Now, your backend should be properly configured and ready to run!
