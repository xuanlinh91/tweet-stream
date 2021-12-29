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

const TWITTER_USER_API = "https://api.twitter.com/2/users/";
const TELEGRAM_BOT_TOKEN = "1907552766:AAGpD0tmzAKfwwut_6alDe8j6N_1EzCFktQ";
const TELEGRAM_CHANNEL_ID = "-570053536";
const TELEGRAM_API = "https://api.telegram.org/bot" + TELEGRAM_BOT_TOKEN + "/sendMessage";

const BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAOv9TgEAAAAAzbML6mMctDAWiFPsZhNKZVcJ6Vg%3DY1idRNejBHf3amwW9fBCskggnDhq3lMMCSCobwGYqiWlQTYWOA";

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

let mySocket = null;
let myStream = null;

const sleep = async (delay) => {
    return new Promise((resolve) => setTimeout(() => resolve(true), delay));
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
        if (response.statusCode === 200) {
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

    const requestConfig = {
        url: rulesURL,
        auth: {
            bearer: BEARER_TOKEN,
        },
        json: true,
    };

    try {
        const response = await get(requestConfig);

        if (response.statusCode !== 200) {
            if (response.statusCode === 403) {
                res.status(403).send(response.body);
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
        }
    } catch (e) {
        res.send(e);
    }
});

const streamTweets = () => {
    const config = {
        url: streamURL,
        auth: {
            bearer: BEARER_TOKEN,
        },
        timeout: 31000,
    };

    try {
        const stream = request.get(config);
        console.log("Start to send stream get request");
        stream
            .on("data", async (data) => {
                try {
                    const json = JSON.parse(data);
                    if (json.connection_issue) {
                        console.log("connection_issue");
                        console.log(json);
                        if (mySocket != null) {
                            mySocket.emit("error", json);
                        }
                        reconnect();
                    } else {
                        if (json.data) {
                            if (mySocket != null) {
                                mySocket.emit("tweet", json);
                            }
                            console.log(json);
                            let authorName = await getTwitterUserName(json.data.author_id);
                            let tweet = authorName + "\n";
                            tweet += json.data.text + "\n";
                            tweet += "https://twitter.com/" + json.data.author_id + "/status/" + json.data.id;
                            sendToTelegram(tweet);
                        } else {
                            console.log("authError");
                            console.log(json);
                            if (mySocket != null) {
                                mySocket.emit("authError", json);
                            }
                            reconnect();
                        }
                    }
                } catch (e) {
                    console.log("exception");
                    console.log(e);
                    if (mySocket != null) {
                        mySocket.emit("heartbeat");
                    }
                }
            })
            .on("error", (error) => {
                // Connection timed out
                console.log("error");
                console.log(error);
                if (mySocket != null) {
                    mySocket.emit("error", errorMessage);
                }

                reconnect();
            });
    } catch (e) {
        console.log("exception 2");
        console.log(e);
        if (mySocket != null) {
            mySocket.emit("exception 2", authMessage);
        }
    }
};

const reconnect = async () => {
    timeout++;
    if (myStream != null) {
        myStream.abort();
    }

    await sleep(2 ** timeout * 1000);
    console.log("reconnect");
    streamTweets();
};

io.on("connection", async (socket) => {
    console.log("On connection");
    try {
        io.emit("connect", "Client connected");
        // streamTweets(io);
        mySocket = io;
    } catch (e) {
        console.log(e);
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
streamTweets();