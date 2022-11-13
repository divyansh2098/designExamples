const express = require("express")
const { Client } = require("pg")
const HashRing = require("hashring")
const { SHA256 } = require("crypto-js")

const app = express()
const hr = new HashRing()

hr.add("5432")
hr.add("5433")
hr.add("5434")

const clients = {
  "5432": new Client({
    host: "divimac",
    port: "5432",
    user: "postgres",
    database: "postgres",
    password: "postgres"
  }),
  "5433": new Client({
    host: "divimac",
    port: "5433",
    user: "postgres",
    database: "postgres",
    password: "postgres"
  }),
  "5434": new Client({
    host: "divimac",
    port: "5434",
    user: "postgres",
    database: "postgres",
    password: "postgres"
  })
}

app.get("/:id", async (req, res) => {
  const urlId = req.params.id
  try {
    const server = hr.get(urlId)
    await clients[server].connect()
    const response = await clients[server].query('SELECT url from url_table where url_id=$1', [urlId])
    await clients[server].end()
    res.status(200).send({
      urlId,
      server,
      url: response.rows[0].url
    })
  } catch (e) {
    res.status(404).send({
      urlId,
      message: "URL for this urlId not found"
    })
  }
})

app.post("/", async (req, res) => {
  try {
    const url = req.query.url
    const urlId = SHA256(url).toString().substring(0, 5)
    const server = hr.get(urlId)
    console.log(server)
    await clients[server].connect()
    const response = await clients[server].query('INSERT INTO url_table(url, url_id) values($1, $2) RETURNING *',  [url, urlId])
    console.log(response)
    await clients[server].end()
    res.status(200).send({
      url,
      urlId,
      server
    })
  } catch (err) {
    throw err
  }
})

app.listen(3000, () => {
  console.log("Listening on port 3000")
})
