// ================ MAPREDUCE FUNCTIONS ==================
function unique_reducer(key, values) {
	return null
}


function count_reducer(key, values) {
	return Array.sum(values)
}


function author_mapper() {
	emit(this.user_id, 1)
}


function hashtag_mapper() {
	for (i in this.hashtags) {
		emit(this.hashtags[i], 1)
	}
}


// =================== HELPER FUNCTIONS ====================
function get_keys(output) {
	keys = []
	for (i in output.results) {
		keys.push(output.results[i]._id)
	}
	return keys
}


function count_sorter(x, y) {
	// note that order is descending
	return (y.value || 0) - (x.value || 0)
}


function get_top_results(results, n) {
	results = results.slice(0)
	results.sort(count_sorter)
	results = results.slice(0, n)
	return results
}


// ================== GENERIC GETTER FUNCTIONS ==================
function get_all_values_raw(tweets, mapper, include_counts) {
	include_counts = typeof include_counts == 'undefined' ? false : include_counts
	return tweets.mapReduce(
		mapper,
		include_counts ? count_reducer : unique_reducer,
		{
			query: {},
			out: { inline: 1 }
		}
	).results
}


function get_all_value_names(tweets, mapper) {
	results = get_all_values_raw(tweets, mapper)
	names = []
	for (i in results) {
		names.push(results[i]._id)
	}
	return names
}


function get_top_values(tweets, mapper, n) {
	n = typeof n == 'undefined' ? 10 : n
	values = get_top_results(get_all_values_raw(tweets, mapper, true), n)
	for (i in values) {
		result = values[i]
		values[i] = { name: result._id, count: result.value }
	}

	return values
}


// ==================== NAMED GETTER FUNCTIONS =================
function get_all_authors(tweets, include_counts) {
	return get_all_value_names(tweets, author_mapper, include_counts)
}


function get_all_hashtags(tweets, include_counts) {
	return get_all_value_names(tweets, hashtag_mapper, include_counts)
}


function get_top_authors(tweets, count) {
	return get_top_values(tweets, author_mapper, count)
}


function get_top_hashtags(tweets, count) {
	return get_top_values(tweets, hashtag_mapper, count)
}
