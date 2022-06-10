import click
import pkg_resources
from cookiecutter.main import cookiecutter

from ..create_designs import create_aisprint_designs

@click.group()
def aisprint_cli():
    pass

@click.command()
@click.option("--application_name", help="Name of the new AI-SPRINT application.", type=str, required=False)
def new_application(application_name):
    # NOTE: maybe better on repository?
    no_input = True if application_name else False
    extra_context = {'application_name': application_name} if application_name else {}
    template_file = pkg_resources.resource_filename('aisprint', 'application_template/application_template.zip')
    cookiecutter(template_file, no_input=no_input, extra_context=extra_context)

@click.command()
@click.option("--application_dir", help="Path to the AI-SPRINT application.", required=True)
@click.option("--output_dir", help="Path to the output folder for the annotations.json file.", required=False)
def create_designs(application_dir, output_dir):
    if not output_dir: 
        output_dir = application_dir
    create_aisprint_designs(output_dir, application_dir)

@click.command()
def run_design_tool():
    # TODO
    pass

aisprint_cli.add_command(create_designs)
aisprint_cli.add_command(run_design_tool)
aisprint_cli.add_command(new_application)

if __name__ == '__main__':
    aisprint_cli()
