FROM node:12.19.0-alpine3.12
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY db-service.js parser.js sjtsk-converter.js docker-run.js package.json package-lock.json ./
RUN npm install
CMD [ "node", "docker-run.js" ]