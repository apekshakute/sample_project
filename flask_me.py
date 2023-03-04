import json

from flask import Flask, request, jsonify

app = Flask(__name__)


@app.route("/")
def flaskme():
    return "hello flask"


@app.route("/hello", methods=['GET'])
def flaskyou():
    name = request.args.get("name")
    return jsonify({"Name": name})

@app.route("/send_data",methods=['POST'])
def send_data():
    data = json.loads(request.data)
    print(type(data))
    return jsonify(data)

if __name__ == "__main__":
    app.run(debug=True)
