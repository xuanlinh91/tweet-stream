const express = require("express");
const bodyParser = require("body-parser");
const util = require("util");
const request = require("request");
const path = require("path");
const socketIo = require("socket.io");
const http = require("http");
const axios = require("axios");

const app = express();
let port = process.env.PORT || 3000;
const post = util.promisify(request.post);
const get = util.promisify(request.get);

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: true}));

const server = http.createServer(app);
const io = socketIo(server);

// const TELEGRAM_BOT_TOKEN = "1977448821:AAE7Oc9qCaKyUiMr1C2BDqt5gzZ_y2TWdV8";
const GIPHY_API = "http://api.giphy.com/v1/gifs/random?api_key=ii6qSOspDIV2E5fn6n8DvSFCSqyVGafd&tag=boobs&rating=r";
const TWITTER_USER_API = "https://api.twitter.com/2/users/";
const TELEGRAM_BOT_TOKEN = "1907552766:AAGpD0tmzAKfwwut_6alDe8j6N_1EzCFktQ";
const TELEGRAM_CHANNEL_ID = "-570053536";
// const TELEGRAM_API = "https://api.telegram.org/bot" + TELEGRAM_BOT_TOKEN + "/sendMessage?chat_id=" + TELEGRAM_CHANNEL_ID + "&text=";
const TELEGRAM_API = "https://api.telegram.org/bot" + TELEGRAM_BOT_TOKEN + "/sendMessage";

const BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAOv9TgEAAAAAzbML6mMctDAWiFPsZhNKZVcJ6Vg%3DY1idRNejBHf3amwW9fBCskggnDhq3lMMCSCobwGYqiWlQTYWOA";

// const BEARER_TOKEN = process.env.TWITTER_BEARER_TOKEN;

let timeout = 0;

const streamURL = new URL(
    "https://api.twitter.com/2/tweets/search/stream?tweet.fields=context_annotations&expansions=author_id"
);

const rulesURL = new URL(
    "https://api.twitter.com/2/tweets/search/stream/rules"
);

const errorMessage = {
    title: "Please Wait",
    detail: "Waiting for new Tweets to be posted...",
};

const authMessage = {
    title: "Could not authenticate",
    details: [
        `Please make sure your bearer token is correct. 
      If using Glitch, remix this app and add it to the .env file`,
    ],
    type: "https://developer.twitter.com/en/docs/authentication",
};

const sleep = async (delay) => {
    return new Promise((resolve) => setTimeout(() => resolve(true), delay));
};

const getGiphyUrl = async () => {
    let requestConfig = {
        url: GIPHY_API,
        json: true,
    };

    try {
        let response = await get(requestConfig);
        if (response.statusCode == 200) {
            let imageUrl = response.body.data.images.downsized_large.url;
            return imageUrl;
        }
    } catch (e) {
        console.log(e);
    }
};

const getTwitterUserName = async (userId) => {
    let requestConfig = {
        url: TWITTER_USER_API + userId,
        id: userId,
        auth: {
            bearer: BEARER_TOKEN,
        },
        json: true,
    };

    try {
        let response = await get(requestConfig);
        if (response.statusCode == 200) {
            let name = response.body.data.name;
            let userName = response.body.data.username;
            return name;
        }
    } catch (e) {
        console.log(e);
    }
};


const sendToTelegram = (message) => {
    axios.post(TELEGRAM_API, {
        chat_id: TELEGRAM_CHANNEL_ID,
        text: message
    })
        .then(function (response) {
            // handle success
            // console.log(response);
        })
        .catch(function (error) {
            // handle error
            console.log(error);
        });
}

app.get("/api/rules", async (req, res) => {
    if (!BEARER_TOKEN) {
        res.status(400).send(authMessage);
    }

    const token = BEARER_TOKEN;
    const requestConfig = {
        url: rulesURL,
        auth: {
            bearer: token,
        },
        json: true,
    };

    try {
        const response = await get(requestConfig);

        if (response.statusCode !== 200) {
            if (response.statusCode === 403) {
                res.status(403).send(response.body);
            } else {
                throw new Error(response.body.error.message);
            }
        }

        res.send(response);
    } catch (e) {
        res.send(e);
    }
});

app.post("/api/rules", async (req, res) => {
    if (!BEARER_TOKEN) {
        res.status(400).send(authMessage);
    }

    const token = BEARER_TOKEN;
    const requestConfig = {
        url: rulesURL,
        auth: {
            bearer: token,
        },
        json: req.body,
    };

    try {
        const response = await post(requestConfig);

        if (response.statusCode === 200 || response.statusCode === 201) {
            res.send(response);
        } else {
            throw new Error(response);
        }
    } catch (e) {
        res.send(e);
    }
});

let myStream = null;
let mySocket = null;

const streamTweets = (token) => {
    const config = {
        url: streamURL,
        auth: {
            bearer: token,
        },
        timeout: 31000,
    };

    try {
        myStream = request.get(config);
        console.log("Start to send stream get request");
        myStream
            .on("data", async (data) => {
                try {
                    const json = JSON.parse(data);
                    if (json.connection_issue) {
                        // console.log(json.connection_issue);
                        mySocket.emit("error", json);
                        reconnect(myStream, token);
                    } else {
                        if (json.data) {
                            mySocket.emit("tweet", json);
                            console.log(json);
                            let authorName = await getTwitterUserName(json.data.author_id);
                            let tweet = authorName + "\n";
                            tweet += json.data.text + "\n";
                            tweet += "https://twitter.com/" + json.data.author_id + "/status/" + json.data.id;
                            sendToTelegram(tweet);
                            // let img = await getGiphyUrl();
                            // sendToTelegram(img);
                        } else {
                            console.log("authError");
                            mySocket.emit("authError", json);
                        }
                    }
                } catch (e) {
                    // console.log(e);
                    mySocket.emit("heartbeat");
                }
            })
            .on("error", (error) => {
                // Connection timed out
                console.log(error);
                mySocket.emit("error", errorMessage);
                reconnect(stream, token);
            });
    } catch (e) {
        console.log(e);
        mySocket.emit("authError", authMessage);
    }
};

const reconnect = async (stream, token) => {
    timeout++;
    stream.abort();
    await sleep(2 ** timeout * 1000);
    console.log("reconnect");
    streamTweets(token);
};

io.on("connection", async (socket) => {
    console.log("On connection");
    try {
        const token = BEARER_TOKEN;
        io.emit("connect", "Client connected");
        mySocket = io;
        streamTweets(token);
    } catch (e) {
        // console.log(e);
        io.emit("authError", authMessage);
    }
});

console.log("NODE_ENV is", process.env.NODE_ENV);

if (process.env.NODE_ENV === "production") {
    app.use(express.static(path.join(__dirname, "../build")));
    app.get("*", (request, res) => {
        res.sendFile(path.join(__dirname, "../build", "index.html"));
    });
} else {
    port = 3001;
}

server.listen(port, () => console.log(`Listening on port ${port}`));
