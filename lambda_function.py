import json
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Users')

def lambda_handler(event, context):
    print("Raw event:", json.dumps(event))

    method = event.get('httpMethod', '')
    raw_path = event.get('path', '')
    print("Full path received by Lambda:", raw_path)

    # Normalize route by stripping stage prefixes like /dev or /api
    route_path = raw_path
    if route_path.startswith("/dev"):
        route_path = route_path[4:]
    if route_path.startswith("/api"):
        route_path = route_path[4:]
    print("Normalized route path:", route_path)

    # Handle CORS preflight
    if method == "OPTIONS":
        return response(200, {"message": "CORS preflight OK"})

    # Route handling
    if method == "POST" and "/login" in route_path:
        return login(event)
    elif method == "POST" and "/changePassword" in route_path:
        return change_password(event)
    elif method == "GET" and "/users" in route_path:
        return get_users()
    elif method == "POST" and "/users" in route_path:
        return add_user(event)
    elif method == "DELETE" and "/users/" in route_path:
        return delete_user(event, route_path)
    else:
        return response(404, {"message": "Invalid route"})

# -------- LOGIN FUNCTION ----------
def login(event):
    try:
        raw_body = event.get('body')
        body = json.loads(raw_body) if raw_body else {}
        username = body.get('Username') or body.get('username')
        password = body.get('Password') or body.get('password')

        print("Login attempt:", username, password)

        if not username or not password:
            return response(400, {"success": False, "message": "Missing Username or Password"})

        res = table.get_item(Key={'Username': username})
        user = res.get('Item')
        print("User from DB:", user)

        if not user:
            return response(400, {"success": False, "message": "User not found"})
        if user.get('Password') != password:
            return response(401, {"success": False, "message": "Invalid password"})

        return response(200, {
            "success": True,
            "message": "Login successful",
            "Username": user['Username'],
            "Role": user.get('Role', 'user'),
            "Status": user.get('Status', 'active')
        })

    except Exception as e:
        print("Unexpected error in login:", e)
        return response(500, {"success": False, "message": "Server error"})

# -------- CHANGE PASSWORD ----------
def change_password(event):
    try:
        body = json.loads(event.get('body', '{}'))
        username = body.get('Username')
        old_password = old_password = body.get('OldPassword')
        new_password = body.get('NewPassword')

        print("Password change attempt:", username)

        if not username or not old_password or not new_password:
            return response(400, {"success": False, "message": "Missing required fields"})

        res = table.get_item(Key={'Username': username})
        user = res.get('Item')

        if not user:
            return response(404, {"success": False, "message": "User not found"})
        if user.get('Password') != old_password:
            return response(401, {"success": False, "message": "Old password incorrect"})

        table.update_item(
            Key={'Username': username},
            UpdateExpression="SET Password = :newpass",
            ExpressionAttributeValues={':newpass': new_password}
        )

        return response(200, {"success": True, "message": "Password updated successfully"})

    except Exception as e:
        print("Error in change_password:", e)
        return response(500, {"success": False, "message": "Server error during password change"})

# -------- GET ALL USERS ----------
def get_users():
    res = table.scan()
    users = res.get('Items', [])
    for u in users:
        u.pop('Password', None)
    return response(200, users)

# -------- ADD USER ----------
def add_user(event):
    body = json.loads(event.get('body', '{}'))
    username = body.get('Username')
    email = body.get('Email')
    role = body.get('Role', 'user')
    status = body.get('Status', 'active')

    if not username or not email:
        return response(400, {"success": False, "message": "Missing Username or Email"})

    table.put_item(Item={
        'Username': username,
        'Password': 'default123',  # default password
        'Email': email,
        'Role': role,
        'Status': status
    })

    return response(200, {"success": True, "message": "User added successfully"})

# -------- DELETE USER ----------
def delete_user(event, route_path):
    try:
        username = route_path.split("/users/")[1]
    except IndexError:
        return response(400, {"success": False, "message": "Username not specified"})
    table.delete_item(Key={'Username': username})
    return response(200, {"success": True, "message": f"User {username} deleted"})

# -------- STANDARD RESPONSE ----------
def response(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,POST,DELETE,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type"
        },
        "body": json.dumps(body)
    }
