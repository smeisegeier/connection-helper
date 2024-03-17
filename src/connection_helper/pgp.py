from dotenv import load_dotenv, find_dotenv
import gnupg as gnu
import os

PASSPHRASE_TEST = "0000"
PRIVATE_KEY_NAME = "PGP_PRIVATE_KEY"

# * create object
gpg = gnu.GPG()


def pgp_generate_key(
    name_email: str = "test@example.com",
    key_length: int = 2048,
    name_real: str = "slim shady",
    name_comment: str = "this is a test key",
    key_type: str = "RSA",
    passphrase: str = PASSPHRASE_TEST,
) -> object:
    """
    Generate a PGP key with the given parameters.

    Args:
        name_email (str): The email address associated with the key. Defaults to "test@example.com".
        key_length (int): The length of the key in bits. Defaults to 2048.
        name_real (str): The real name associated with the key. Defaults to "slim shady".
        name_comment (str): A comment associated with the key. Defaults to "this is a test key".
        key_type (str): The type of key to generate. Defaults to "RSA".
        passphrase (str): The passphrase to protect the key. Defaults to PASSPHRASE_TEST.

    Returns:
        object: The generated PGP key object.
    """
    # * setup config
    input_data = gpg.gen_key_input(
        name_email=name_email,
        key_length=key_length,
        name_real=name_real,
        name_comment=name_comment,
        key_type=key_type,
        passphrase=passphrase,
    )

    # * generate raw key object
    key = gpg.gen_key(input_data)

    return key


def pgp_export_public_key(key: object, output_file_path: str = None) -> str:
    """
    A function to export a public key from an object id.
    
    Parameters:
        key (object): The object id from which the public key will be exported.
        output_file_path (str, optional): The file path where the public key will be saved. Defaults to None.
    
    Returns:
        str: The exported public key.
    """
    # * export public key from object id
    public_key = gpg.export_keys(key.__str__(), secret=False)

    # * print public key to have copyable text
    print(public_key)
    
    if output_file_path:
        with open(output_file_path, "w") as f:
            f.write(public_key)

    return public_key


def pgp_export_private_key(
    key: object, passphrase: str = PASSPHRASE_TEST
) -> str:
    """
    Export a private key from the given key object.

    Args:
        key (object): The key object to export the private key from.
        passphrase (str, optional): The passphrase to unlock the private key. Defaults to PASSPHRASE_TEST.

    Returns:
        str: The exported private key.

    """
    private_key = gpg.export_keys(key.__str__(), secret=True, passphrase=passphrase)

    return private_key

def pgp_import_key(key: str, key_file_path: str=None) -> object:
    """
    A function that imports a PGP key either from a string or a file path and returns the import results. Either public or private key can be imported.
    
    Parameters:
    key (str): The PGP key to import, either passed as a string or read from a file.
    key_file_path (str, optional): The file path to the PGP key file. If provided, the function reads the key from this file.
    
    Returns:
    object: The import results of the PGP key.
    """

    if key_file_path:
        with open(key_file_path, "r") as f:
            key = f.read()

    result = gpg.import_keys(key)
    print(result.results)
    return result.results

def pgp_encrypt(
    message: str,
    recipient_key_id_list: str | list[str],
    passphrase: str = PASSPHRASE_TEST,
    output_file_path: str = None,
) -> str:
    """
    Encrypts a message using PGP encryption for the specified recipient key IDs and optional passphrase.
    
    Args:
        message (str): The message to be encrypted.
        recipient_key_id_list (str | list[str]): The recipient key ID(s) for encryption.
        passphrase (str, optional): The passphrase for encryption. Defaults to PASSPHRASE_TEST.
        output_file_path (str, optional): The file path to save the encrypted message. Defaults to None.
    
    Returns:
        str: The encrypted message.
    """

    if not isinstance(recipient_key_id_list, list):
        recipient_key_id_list = [recipient_key_id_list]

    result = gpg.encrypt(
        data=message,
        recipients=recipient_key_id_list,
        passphrase=passphrase,
        always_trust=True,  # ! if false, keys trust must be ultimate
    )

    out = result.data.decode("utf-8")
    print(f"message for recipient(s) {recipient_key_id_list}:\n{out}")

    if output_file_path:
        with open(output_file_path, "w") as f:
            # * decode byte stream to string
            f.write(out)

    return result


def pgp_decrypt(
    message: str = None,
    message_file_path: str = None,
    passphrase: str = PASSPHRASE_TEST,
) -> str:
    """
    A function to decrypt a PGP message. Decrypts the provided message using a passphrase and returns the decrypted message as a string.
    Private key of recipient must be in keyring!

    Parameters:
    - message: str, the PGP message to decrypt
    - message_file_path: str, optional, the file path to the PGP message to decrypt
    - passphrase: str, the passphrase required for decryption

    Returns:
    - str: The decrypted message as a string
    """

    if message_file_path:
        with open(message_file_path) as f:
            message = f.read()

    if not message:
        print("❌ no message or message file to decrypt")
        return None

    # * decrypt using secret key
    orig = gpg.decrypt(message, 
                    passphrase=passphrase, 
                    always_trust=False,
                )

    # * decode byte stream to string
    out = orig.data.decode("utf-8")
    print(out)

    return orig
