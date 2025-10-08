import json
import boto3
from decimal import Decimal
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Users')

def lambda_handler(event, context):
    print("Event:", event)
    method = event['httpMethod']
    path = event['path']

    if method == "POST" and path.endswith("/login"):
        return login(event)
    elif method == "GET" and path.endswith("/users"):
        return get_users(event)
    elif method == "POST" and path.endswith("/users"):
        return add_user(event)
    elif method == "DELETE" and "/users/" in path:
        return delete_user(event)
    else:
        return response(404, {"message": "Invalid route"})

def login(event):
    body = json.loads(event['body'])
    username = body.get('username')
    password = body.get('password')

    res = table.get_item(Key={'username': username})
    user = res.get('Item')

    if not user:
        return response(400, {"success": False, "message": "User not found"})
    if user['password'] != password:
        return response(401, {"success": False, "message": "Invalid password"})

    return response(200, {
        "success": True,
        "message": "Login successful",
        "username": user['username'],
        "role": user['role']
    })

def get_users(event):
    res = table.scan()
    users = res.get('Items', [])
    for u in users:
        u.pop('password', None)
    return response(200, users)

def add_user(event):
    body = json.loads(event['body'])
    username = body.get('username')
    email = body.get('email')
    role = body.get('role', 'user')

    if not username or not email:
        return response(400, {"success": False, "message": "Missing username or email"})

    table.put_item(Item={
        'username': username,
        'password': 'default123',
        'email': email,
        'role': role,
        'status': 'active',
        'created_at': datetime.utcnow().isoformat()
    })

    return response(200, {"success": True, "message": "User added successfully"})

def delete_user(event):
    username = event['pathParameters']['username']
    table.delete_item(Key={'username': username})
    return response(200, {"success": True, "message": f"User {username} deleted"})

def response(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,POST,DELETE,OPTIONS"
        },
        "body": json.dumps(body, default=str)
    }

