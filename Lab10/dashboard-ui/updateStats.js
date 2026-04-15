/* UPDATE THESE VALUES TO MATCH YOUR SETUP */

const PROCESSING_STATS_API_URL = "http://localhost:8100/stats"
const ANALYZER_API_URL = {
    stats:          "http://localhost:8025/stats",
    match_history:  "http://localhost:8025/league/match_history?index=0",
    champion_winrate: "http://localhost:8025/league/champion_winrate?index=0"
}

// This function fetches and updates the general statistics
const makeReq = (url, cb) => {
    fetch(url)
        .then(res => res.json())
        .then((result) => {
            console.log("Received data: ", result)
            cb(result);
        }).catch((error) => {
            updateErrorMessages(error.message)
        })
}

const updateCodeDiv = (result, elemId) => document.getElementById(elemId).innerText = JSON.stringify(result, null, 2)

const getLocaleDateStr = () => (new Date()).toLocaleString()

const getStats = () => {
    document.getElementById("last-updated-value").innerText = getLocaleDateStr()

    makeReq(PROCESSING_STATS_API_URL,              (result) => updateCodeDiv(result, "processing-stats"))
    makeReq(ANALYZER_API_URL.stats,                (result) => updateCodeDiv(result, "analyzer-stats"))
    makeReq(ANALYZER_API_URL.match_history,        (result) => updateCodeDiv(result, "event-match-history"))
    makeReq(ANALYZER_API_URL.champion_winrate,     (result) => updateCodeDiv(result, "event-champion-winrate"))
}

const updateErrorMessages = (message) => {
    const id = Date.now()
    console.log("Creation", id)
    msg = document.createElement("div")
    msg.id = `error-${id}`
    msg.innerHTML = `<p>Something happened at ${getLocaleDateStr()}!</p><code>${message}</code>`
    document.getElementById("messages").style.display = "block"
    document.getElementById("messages").prepend(msg)
    setTimeout(() => {
        const elem = document.getElementById(`error-${id}`)
        if (elem) { elem.remove() }
    }, 7000)
}

const setup = () => {
    getStats()
    setInterval(() => getStats(), 4000) // Update every 4 seconds
}

document.addEventListener('DOMContentLoaded', setup)
