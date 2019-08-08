FROM node
WORKDIR /home/app
COPY package*.json ./
RUN npm install
RUN npm audit fix