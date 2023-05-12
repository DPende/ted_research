const connect_to_db = require('./db');
const https = require('https');

// GET BY TALK HANDLER

const talk = require('./Talk');

function getRequest(uri) {

    var options = {
        'headers': {
            'Authorization': `Bearer ${process.env.API}`
        },
        'maxRedirects': 20
    };

    return new Promise((resolve, reject) => {
        const req = https.get(uri, options, res => {
            let rawData = '';

            res.on('data', chunk => {
                rawData += chunk;
            });

            res.on('end', () => {
                try {
                    resolve(JSON.parse(rawData));
                }
                catch (err) {
                    reject(new Error(err));
                }
            });
        });

        req.on('error', err => {
            reject(new Error(err));
        });
    });
}

module.exports.get_correlated_papers_by_idx = async (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));
    let body = {}
    if (event.body) {
        body = JSON.parse(event.body)
    }
    // set default
    if (!body.idx) {
        callback(null, {
            statusCode: 500,
            headers: { 'Content-Type': 'text/plain' },
            body: 'Could not fetch the talks. Idx is null.'
        })
    }

    if (!body.doc_per_page) {
        body.doc_per_page = 10
    }
    if (!body.page) {
        body.page = 1
    }

    try {
        await connect_to_db();
        console.log('=> get_all talks');
        const talks = await talk.findById(body.idx);

        var tagsForQuery = talks.tags.map(tag => `(${tag})`);

        const uri = `https://api.core.ac.uk/v3/search/journals/?q=${encodeURIComponent(tagsForQuery.join('OR'))}`

        const result = await getRequest(uri);

        callback(null, {
            statusCode: 200,
            body: JSON.stringify(result),
        });
    }
    catch (error) {
        console.error(error);
        callback(null, {
            statusCode: 400,
            body: JSON.stringify(error),
        });
    }
};
