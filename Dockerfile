# Build stage: compile TypeScript to JavaScript.
FROM node:22-alpine AS build

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci || npm install

COPY tsconfig.json ./
COPY src ./src
RUN npm run build

# Prune to production dependencies only.
RUN npm prune --omit=dev

# Runtime stage.
FROM node:22-alpine

RUN addgroup -S spark && adduser -S spark -G spark

WORKDIR /app

COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY package.json ./
COPY config.yaml ./

USER spark

ENV NODE_ENV=production \
    SHS_MCP__ADDRESS=0.0.0.0 \
    SHS_MCP__PORT=18888

EXPOSE 18888

ENTRYPOINT ["node", "dist/index.js"]
