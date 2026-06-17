import os
import sys

from pathlib import Path
from pprint import pprint

from cognite.client import CogniteClient, ClientConfig
from cognite.client.credentials import OAuthClientCredentials

# This is necessary to import adjacent modules in the function code.
sys.path.insert(0, str(Path(__file__).parent / "local_code"))

from local_code.handler import handle # noqa: E402

try:
    from dotenv import load_dotenv

    for parent in Path(__file__).resolve().parents:
        if (parent / ".env").exists():
            load_dotenv(parent / '.env')
except ImportError:
    ...


def main() -> None:
    credentials = OAuthClientCredentials(
        token_url="https://login.microsoftonline.com/47e6bc76-3294-480a-b5b8-6f2a3e82dc86/oauth2/v2.0/token",
        client_id="52751be0-6b4b-40e6-8d50-7cdcdae5732a",
        client_secret=os.environ["IDP_CLIENT_SECRET"],
        scopes=['https://bluefield.cognitedata.com/.default'],
    )

    client = CogniteClient(
        config=ClientConfig(
            client_name="CDF-Toolkit:0.6.53",
            project="ganesh-project",
            base_url="https://bluefield.cognitedata.com",
            credentials=credentials,
        )
    )

    print("icapi_datapoints_extractor LOGS:")
    response = handle(
        client=client,
        data={},
    )

    print("icapi_datapoints_extractor RESPONSE:")
    pprint(response)


if __name__ == "__main__":
    main()
