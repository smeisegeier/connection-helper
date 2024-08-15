from infisical_client import (
    ClientSettings,
    InfisicalClient,
    GetSecretOptions,
    AuthenticationOptions,
    UniversalAuthMethod,
)
from dotenv import load_dotenv
import os

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