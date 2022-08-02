FROM node:16-alpine
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY db-service.js parser.js sjtsk-converter.js docker-run.js package.json package-lock.json ./
RUN npm install
CMD [ "node", "docker-run.js" ]