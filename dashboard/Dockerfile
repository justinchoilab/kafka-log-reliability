FROM node:20-alpine
WORKDIR /app
COPY package.json .
RUN npm install --omit=dev
COPY src/ src/
COPY public/ public/
RUN cp node_modules/chart.js/dist/chart.umd.min.js public/chart.min.js
EXPOSE 3000
CMD ["node", "src/server.js"]
