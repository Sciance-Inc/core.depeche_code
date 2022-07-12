from onepasswordconnectsdk.client import new_client_from_environment

# Fetch the secret values
def fetch_secret(secret_name: str, vault_name: str = "maas"):
    """
    Fetch a secret from the Onepassword Connect API

    Args:
        secret_name (str): The name of the secret to fetch
    """
    client = new_client_from_environment()
    vault_ids = [vault.id for vault in client.get_vaults() if vault.name == vault_name]
    secret = client.get_item_by_title(secret_name, vault_ids[0]).fields
    secret = {item.label: item for item in secret}

    return secret
