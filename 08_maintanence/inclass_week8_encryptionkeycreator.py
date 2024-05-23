from google.cloud import storage
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

# Generate RSA keys
private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
public_key = private_key.public_key()

# Serialize the private key
private_key_pem = private_key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.TraditionalOpenSSL,
    encryption_algorithm=serialization.NoEncryption()
)

# Serialize the public key
public_key_pem = public_key.public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo
)

# Save keys to Google Cloud Storage
storage_client = storage.Client()
bucket_name = 'cs510-spring24-project1-bucket'
bucket = storage_client.bucket(bucket_name)

# Upload private key
private_key_blob = bucket.blob('keys/private_key.pem')
private_key_blob.upload_from_string(private_key_pem)

# Upload public key
public_key_blob = bucket.blob('keys/public_key.pem')
public_key_blob.upload_from_string(public_key_pem)

print("Keys generated and uploaded to Google Cloud Storage")

