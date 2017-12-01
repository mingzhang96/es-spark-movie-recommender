from elasticsearch import Elasticsearch

es = Elasticsearch()

# your key
API_KEY = "YOUR KEY"
# your index name
indexName = "demo2"

def get_poster_url(id):
    """Fetch movie poster image URL from TMDb API given a tmdbId"""
    IMAGE_URL = 'https://image.tmdb.org/t/p/w500'
    try:
        import tmdbsimple as tmdb
        from tmdbsimple import APIKeyError
        try:
            tmdb.API_KEY = API_KEY
            movie = tmdb.Movies(id).info()
            poster_url = IMAGE_URL + movie['poster_path'] if 'poster_path' in movie and movie['poster_path'] is not None else ""
            return poster_url
        except APIKeyError as ae:
            return "KEY_ERR"
    except Exception as me:
        return "NA"


def reverse_convert(s):
    '''Convert a delimited token filter format string back to list format'''
    return [float(f.split("|")[1]) for f in s.split(" ")]


def fn_query(query_vec, q="*", cosine=False):
    """
    Construct an Elasticsearch function score query.

    The query takes as parameters:
        - the field in the candidate document that contains the factor vector
        - the query vector
        - a flag indicating whether to use dot product or cosine similarity (normalized dot product) for scores

    The query vector passed in will be the user factor vector (if generating recommended movies for a user)
    or movie factor vector (if generating similar movies for a given movie)
    """
    return {
        "query": {
            "function_score": {
                "query": {
                    "query_string": {
                        "query": q
                    }
                },
                "script_score": {
                    "script": {
                        "inline": "payload_vector_score",
                        "lang": "native",
                        "params": {
                            "field": "@model.factor",
                            "vector": query_vec,
                            "cosine": cosine
                        }
                    }
                },
                "boost_mode": "replace"
            }
        }
    }


def get_similar(the_id, q="*", num=10, index="demo2", dt="movies"):
    """
    Given a movie id, execute the recommendation function score query to find similar movies, ranked by cosine similarity
    """
    response = es.get(index=index, doc_type=dt, id=the_id)
    print("=========get response", response)
    src = response['_source']
    if '@model' in src and 'factor' in src['@model']:
        raw_vec = src['@model']['factor']
        # our script actually uses the list form for the query vector and handles conversion internally
        query_vec = reverse_convert(raw_vec)
        q = fn_query(query_vec, q=q, cosine=True)
        results = es.search(index, dt, body=q)
        hits = results['hits']['hits']
        return src, hits[1:num + 1]


def get_user_recs(the_id, q="*", num=10, index=indexName):
    """
    Given a user id, execute the recommendation function score query to find top movies, ranked by predicted rating
    """
    response = es.get(index=index, doc_type="users", id=the_id)
    src = response['_source']
    if '@model' in src and 'factor' in src['@model']:
        raw_vec = src['@model']['factor']
        # our script actually uses the list form for the query vector and handles conversion internally
        query_vec = reverse_convert(raw_vec)
        q = fn_query(query_vec, q=q, cosine=False)
        results = es.search(index, "movies", body=q)
        hits = results['hits']['hits']
        return src, hits[:num]


def get_movies_for_user(the_id, num=10, index=indexName):
    """
    Given a user id, get the movies rated by that user, from highest- to lowest-rated.
    """
    response = es.search(index=index, doc_type="ratings", q="userId:%s" % the_id, size=num, sort=["rating:desc"])
    hits = response['hits']['hits']
    ids = [h['_source']['movieId'] for h in hits]
    movies = es.mget(body={"ids": ids}, index=index, doc_type="movies", _source_include=['tmdbId', 'title'])
    movies_hits = movies['docs']
    tmdbids = [h['_source'] for h in movies_hits]
    return tmdbids


def display_user_recs(the_id, q="*", num=10, num_last=10, index=indexName):
    user, recs = get_user_recs(the_id, q, num, index)
    user_movies = get_movies_for_user(the_id, num_last, index)
    # check that posters can be displayed
    first_movie = user_movies[0]
    first_im_url = get_poster_url(first_movie['tmdbId'])
    if first_im_url == "NA":
        print("Cannot import tmdbsimple. No movie posters will be displayed!")
    if first_im_url == "KEY_ERR":
        print("Key error accessing TMDb API. Check your API key. No movie posters will be displayed!")

    # display the movies that this user has rated highly
    print("Get recommended movies for user id %s" % the_id)
    print("The user has rated the following movies highly:")
    user_html = "<table border=0>"
    i = 0
    for movie in user_movies:
        movie_im_url = get_poster_url(movie['tmdbId'])
        movie_title = movie['title']
        user_html += "movie's title & url: %s %s" % (movie_title, movie_im_url)
        i += 1
    print(user_html)
    # now display the recommended movies for the user
    print("Recommended movies:")
    i = 0
    for rec in recs:
        r_im_url = get_poster_url(rec['_source']['tmdbId'])
        r_score = rec['_score']
        r_title = rec['_source']['title']
        print("recommend title & url & score: %s ---- %s ---- %2.3f" % (
            r_title, r_im_url, r_score))
        i += 1


def display_similar(the_id, q="*", num=10, index="demo2", dt="movies"):
    """
    Display query movie, together with similar movies and similarity scores, in a table
    """
    movie, recs = get_similar(the_id, q, num, index, dt)
    q_im_url = get_poster_url(movie['tmdbId'])
    if q_im_url == "NA":
        print("Cannot import tmdbsimple. No movie posters will be displayed!")
    if q_im_url == "KEY_ERR":
        print("Key error accessing TMDb API. Check your API key. No movie posters will be displayed!")

    print("Get similar movies for:")
    print("%s" % movie['title'])
    print("People who liked this movie also liked these:")
    i = 0
    for rec in recs:
        r_im_url = get_poster_url(rec['_source']['tmdbId'])
        r_score = rec['_score']
        r_title = rec['_source']['title']
        print("recommend title & url & score: %s ---- %s ---- %2.3f" % (
            r_title, r_im_url, r_score))
        i += 1


display_similar(2628, num=5)
print("")
display_similar(2628, num=5, q="title:(NOT matrix)")
print("")
display_similar(1, num=5, q="genres:children")
print("")
display_user_recs(12, num=5, num_last=5)
print("")
display_user_recs(12, num=5, num_last=5, q="release_date:[2012 TO *]")
