import typer

from ..libs.helpers import s3_filesystem_destination
from . import biodiversa, cordis, erc, eurlex, keep, life

app = typer.Typer(help="Sync data from EU Project/Funding Databases")


@app.callback()
def main(
    ctx: typer.Context,
    endpoint_url: str = typer.Option(
        ...,
        envvar="TOKENS_AWS_ENDPOINT",
        help="AWS S3 endpoint URL",
    ),
    access_key: str = typer.Option(
        ...,
        envvar="TOKENS_AWS_ACCESS_KEY",
        help="AWS S3 access key",
    ),
    secret_key: str = typer.Option(
        ...,
        envvar="TOKENS_AWS_SECRET_KEY",
        help="AWS S3 secret key",
    ),
    bucket: str = typer.Option(
        ...,
        envvar="TOKENS_AWS_BUCKET",
        help="AWS S3 bucket name",
    ),
    prefix: str = typer.Option(
        default="tokens",
        envvar="TOKENS_AWS_PREFIX",
        help="AWS S3 prefix (folder path) for storing data",
    ),
    region: str = typer.Option(
        default="us-east-1",
        envvar="TOKENS_S3_REGION",
        help="AWS S3 region",
    ),
):
    """Build the shared S3 bucket URL and filesystem destination for token sources."""
    bucket_url, filesystem_destination = s3_filesystem_destination(
        endpoint_url, access_key, secret_key, bucket, prefix, region
    )
    ctx.obj = {
        "bucket_url": bucket_url,
        "filesystem_destination": filesystem_destination,
    }


app.add_typer(biodiversa.app, name="biodiversa")
app.add_typer(cordis.app, name="cordis")
app.add_typer(erc.app, name="erc")
app.add_typer(eurlex.app, name="eurlex")
app.add_typer(keep.app, name="keep")
app.add_typer(life.app, name="life")

if __name__ == "__main__":
    app()
