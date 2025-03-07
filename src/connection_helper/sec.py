from infisical_client import (
    ClientSettings,
    InfisicalClient,
    GetSecretOptions,
    AuthenticationOptions,
    UniversalAuthMethod,
)
from dotenv import load_dotenv, find_dotenv
import os

from bitwarden_sdk import BitwardenClient, DeviceType, client_settings_from_dict


def get_infisical_secrets(
    secrets: list[str],
    env_path: str = "",
    environment="dev",
) -> list[str]:
    """
    ! Note: this function must be extra installed with `pip install 'connection-helper[sec]'` (mind the extra '')
    A function to retrieve secrets from InfisicalClient based on the provided settings.
    A path to .env file must be provided, containing all of these items:
    INF_PROJECT, INF_CLIENT, INF_SECRET

    Args:
        secrets (list[str]): The list of secret names to retrieve.
        env_path (str, optional): The path to the environment file. Defaults to "".
        environment (str, optional): The environment to use, e.g., "dev". Defaults to "dev".

    Returns:
        list[str]: A list of secret values retrieved from InfisicalClient.
    """

    if not load_dotenv(env_path):
        print("❌ missing .env file")
        return None
    else:
        client_id = os.environ["INF_CLIENT"]
        client_secret = os.environ["INF_SECRET"]
        project_id = os.environ["INF_PROJECT"]

    if not all([client_id, client_secret, project_id]):
        print("❌ missing items in .env file")
        return None

    client = InfisicalClient(
        ClientSettings(
            auth=AuthenticationOptions(
                universal_auth=UniversalAuthMethod(
                    client_id=client_id,
                    client_secret=client_secret,
                )
            )
        )
    )

    out = []
    for secret in secrets:
        result = client.getSecret(
            options=GetSecretOptions(
                environment=environment,
                project_id=project_id,
                secret_name=secret,
            )
        )
        out.append(result.secret_value)

    return out

def get_bitwarden_secrets(list_key_ids: list[str]):
    
    """
    Retrieve secrets from Bitwarden.

    This function takes a list of key_ids and returns a dictionary with the secret values.
    The Bitwarden API access token is loaded from the .env file, that must contain the BWS_ACCESS_TOKEN

    Parameters:
    - list_key_ids: list[str], the list of key_ids to retrieve

    Returns:
    - dict, the list of dictionarys with the secret values

    Remarks:
    - requires a .env file with the BWS_ACCESS_TOKEN
    - requires the bitwarden-sdk package
    """
    if not load_dotenv(find_dotenv()):
        print("❌ missing .env file")
        return None
    if not os.getenv("BWS_ACCESS_TOKEN"):
        print("❌ missing BWS_ACCESS_TOKEN")
        return None
    if list_key_ids is None:
        print("❌ missing list_key_ids")
        return None

    _=load_dotenv(find_dotenv())

    identityUrl = "https://identity.bitwarden.com"
    apiUrl = "https://api.bitwarden.com"
    accessToken = os.getenv("BWS_ACCESS_TOKEN")

    client = BitwardenClient(
        client_settings_from_dict(
            {
                "apiUrl": apiUrl,
                "deviceType": DeviceType.SDK,
                "identityUrl": identityUrl,
                "userAgent": "Python",
            }
        )
    )

    response=client.auth().login_access_token(access_token=accessToken)
    # print(response.success)
    
    secrets_response = client.secrets().get_by_ids(list_key_ids).data.to_dict()
    
    return secrets_response
