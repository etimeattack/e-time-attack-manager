import logging
import click

from report_generators.events import EventsParser
from report_generators.laps import LapsParser

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@click.command()
@click.option('--event-id', required=True, help='The number of the event in the series')
@click.option('--sectors-amount', default=3, help='The amount of sectors in the selected track')
def generate_report(event_id: int, sectors_amount: int):
    logger.info("Report generation started")

    EventsParser(event_id, 'per_driver_events').generate()
    LapsParser(event_id, 'per_driver_stats', sectors_amount=sectors_amount).generate()

    logger.info("Report generation finished successfully")


if __name__ == '__main__':
    generate_report()
