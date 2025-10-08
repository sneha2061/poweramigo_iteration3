import json
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Users')

def lambda_handler(event, context):
    print("Event:", event)
    method = event.get('httpMethod', '')
    path = event.get('path', '')

# Handle CORS preflight
    if method == "OPTIONS":
        return response(200, {"message": "CORS preflight OK"})
    if method == "POST" and path.endswith("/login"):
        return login(event)
    elif method == "GET" and path.endswith("/users"):
        return get_users()
    elif method == "POST" and path.endswith("/users"):
        return add_user(event)
    elif method == "DELETE" and "/users/" in path:
        return delete_user(event)
    else:
        return response(404, {"message": "Invalid route"})

# -------- LOGIN FUNCTION ----------
def login(event):
    body = json.loads(event['body'])
    username = body.get('Username')
    password = body.get('Password')

    if not username or not password:
        return response(400, {"success": False, "message": "Missing Username or Password"})

    res = table.get_item(Key={'Username': username})
    user = res.get('Item')

    if not user:
        return response(400, {"success": False, "message": "User not found"})
    if user['Password'] != password:
        return response(401, {"success": False, "message": "Invalid password"})

    return response(200, {
        "success": True,
        "message": "Login successful",
        "Username": user['Username'],
        "Role": user['Role'],
        "Status": user['Status']
    })

# -------- GET ALL USERS ----------
def get_users():
    res = table.scan()
    users = res.get('Items', [])
    for u in users:
        u.pop('Password', None)
    return response(200, users)

# -------- ADD USER ----------
def add_user(event):
    body = json.loads(event['body'])
    username = body.get('Username')
    email = body.get('Email')
    role = body.get('Role', 'user')
    status = body.get('Status', 'active')

    if not username or not email:
        return response(400, {"success": False, "message": "Missing Username or Email"})

    table.put_item(Item={
        'Username': username,
        'Password': 'default123',  # Default password for new users
        'Email': email,
        'Role': role,
        'Status': status
    })

    return response(200, {"success": True, "message": "User added successfully"})

# -------- DELETE USER ----------
def delete_user(event):
    username = event['pathParameters']['username']
    table.delete_item(Key={'Username': username})
    return response(200, {"success": True, "message": f"User {username} deleted"})

# -------- STANDARD RESPONSE ----------
def response(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,POST,DELETE,OPTIONS"
        },
        "body": json.dumps(body)
    }
