const mongoose = require('mongoose');

const talk_schema = new mongoose.Schema({
    _id: String,
    main_speaker: String,
    title: String,
    url: String,
    posted: String,
    details: String,
    main_author: String,
    num_views: Number,
    watch_nexts: Array,
    tags: Array,
    papers: Array
}, { collection: 'tedx_data' });

module.exports = mongoose.model('talk', talk_schema);

