from flask import Flask, request
from elasticsearch import Elasticsearch

app = Flask(__name__)
es = Elasticsearch(['elasticsearch:9200'])

@app.route('/search', methods=['GET'])
def search():
    rdata = request.args
    query_text = rdata.get('text')
    query_username = rdata.get('username')
    query_min_likes = rdata.get('min_likes')
    query_min_followers = rdata.get('min_followers')
    must = []
    if query_text:
        must.append({"match": {"text": query_text}})
    if query_username:
        must.append({"match_phrase": {"user.username": query_username}})
    if query_min_likes:
        must.append({"range": {"like_count": {"gte": query_min_likes}}})
    if query_min_followers:
        must.append({"range": {"user.follower_count": {"gte": query_min_followers}}})

    if not must:
        must.append({"match_all": {}})
    body = {"query": {"bool": {"must": must}}}
    response = es.search(index="users-posts", body=body)
    return response['hits']

@app.route('/tag', methods=['POST'])
def tag():
    rdata = request.json
    id = str(rdata["id"])
    tags = [str(t) for t in rdata["tags"]]
    response = es.update(index="users-posts", id=id, body={"tags": tags})
    return {'success': True}

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
