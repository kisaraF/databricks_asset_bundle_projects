import click


@click.group()
def fda_animals():
    pass


@click.command()
@click.option("--catalog_name", required=True)
@click.option("--schema_name", required=True)
@click.option("--table_name", required=True)
@click.option(
    "--load_n", default=100, type=int, required=False, help="how much points to load"
)
def extract(catalog_name, schema_name, table_name, load_n):
    from fda_animals.main import load_data

    load_data(catalog_name, schema_name, table_name, load_n)


fda_animals.add_command(extract)
