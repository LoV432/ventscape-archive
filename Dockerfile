FROM node:20-slim AS app

ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD true
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

RUN apt-get update && \
    apt-get install -y wget gnupg && \
    apt-get install -y fonts-ipafont-gothic fonts-wqy-zenhei fonts-thai-tlwg fonts-kacst fonts-freefont-ttf libxss1 \
    libgtk2.0-0 libnss3 libatk-bridge2.0-0 libdrm2 libxkbcommon0 libgbm1 libasound2 && \
    apt-get install -y chromium && \
    apt-get clean

RUN mkdir -p /home/node/app/node_modules && mkdir -p /home/node/app/logs && chown -R 1000:1000 /home/node/app

WORKDIR /home/node/app


USER 1000

COPY --chown=1000:1000 package*.json ./
RUN npm install

COPY --chown=1000:1000 ./*.ts ./

CMD [ "npx", "tsx", "index.ts" ]