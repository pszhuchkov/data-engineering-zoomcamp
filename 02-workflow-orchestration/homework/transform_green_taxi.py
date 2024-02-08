if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]

    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    data.columns = (
        data.columns.str.replace(' ', '_').str.lower()
    )

    return data
    


@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
    assert all(elem in [1,2] for elem in output['vendorid'].unique()), 'Only 1 and 2 vendors are allowed'
    assert len(output[output['passenger_count'] < 1 ]) == 0, 'Passengers count must be >= 1'
    assert len(output[output['trip_distance'] <= 0 ]) == 0, 'Trip distance count must be > 0'
