from syscore.exceptions import missingData
from sysobjects.contract_dates_and_expiries import expiryDate
from sysbrokers.IB.ib_futures_contract_price_data import (
    futuresContract,
)
from syscore.dateutils import DAILY_PRICE_FREQ, HOURLY_FREQ, Frequency
from sysdata.data_blob import dataBlob


from sysdata.mongodb.mongo_futures_contracts import mongoFuturesContractData

from sysproduction.data.broker import dataBroker
from sysproduction.data.prices import updatePrices
from sysproduction.update_historical_prices import write_merged_prices_for_contract


def seed_price_data_from_IB(instrument_code, mongo_contract_data: mongoFuturesContractData, data, data_broker):


    list_of_contracts = data_broker.get_list_of_contract_dates_for_instrument_code(
        instrument_code, allow_expired=True
    )

    ## This returns yyyymmdd strings, where we have the actual expiry date
    for contract_date in list_of_contracts:
        ## We do this slightly tortorous thing because there are energy contracts
        ## which don't expire in the month they are labelled with
        ## So for example, CRUDE_W 202106 actually expires on 20210528

        expiry_date = expiryDate.from_str(contract_date)
        if expiry_date > expiryDate.from_str('20241231'):
            continue
        date_str = contract_date[:6]
        contract_object = futuresContract(instrument_code, date_str)
        log = contract_object.specific_log(data.log)

        try:

            seed_price_data_for_contract(data=data, contract_object=contract_object, log=log)

        except Exception as ex:
            log.error(f'Error seeding price data for {instrument_code=} {contract_date=} {ex.args=}')
            continue

        if mongo_contract_data.is_contract_in_data(instrument_code, date_str):

            log.info(f'Not adding contract {contract_object=} {contract_date=} as already present in data')
            continue

        contract_object.update_single_expiry_date(expiry_date)

        log.info(f'adding contract {contract_object=} {contract_date=}')
        mongo_contract_data.add_contract_data(contract_object)


def seed_price_data_for_contract(data: dataBlob, contract_object: futuresContract, log):

    list_of_frequencies = [HOURLY_FREQ, DAILY_PRICE_FREQ]
    for frequency in list_of_frequencies:
        log.debug("Getting data at frequency %s" % str(frequency))
        seed_price_data_for_contract_at_frequency(
            data=data, contract_object=contract_object, frequency=frequency
        )

    log.debug("Writing merged data for %s" % str(contract_object))
    write_merged_prices_for_contract(
        data, contract_object=contract_object, list_of_frequencies=list_of_frequencies
    )


def seed_price_data_for_contract_at_frequency(
    data: dataBlob, contract_object: futuresContract, frequency: Frequency
):

    data_broker = dataBroker(data)
    update_prices = updatePrices(data)
    log = contract_object.specific_log(data.log)

    try:
        prices = (
            data_broker.get_prices_at_frequency_for_potentially_expired_contract_object(
                contract_object, frequency=frequency
            )
        )
    except missingData:
        log.warning("Error getting data for %s" % str(contract_object))
        return None

    log.debug("Got %d lines of prices for %s" % (len(prices), str(contract_object)))

    if len(prices) == 0:
        log.warning("No price data for %s" % str(contract_object))
    else:
        update_prices.overwrite_prices_at_frequency_for_contract(
            contract_object=contract_object, frequency=frequency, new_prices=prices
        )


if __name__ == "__main__":
    import argparse

    app = argparse.ArgumentParser()

    contract_data = mongoFuturesContractData()

    app.add_argument('--instrument-codes', nargs='+')

    args = app.parse_args()

    data = dataBlob()
    data_broker = dataBroker(data)

    for instrument_code in args.instrument_codes:

        try:

            seed_price_data_from_IB(instrument_code, contract_data, data, data_broker)

        except Exception as ex:

            print(f'Error seeding data for {instrument_code=}, {ex.args=}')
