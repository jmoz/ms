import pytest


# At MayStreet, we capture market data from many different exchanges around the world.
# All of these exchanges transmit data in similar ways. Notably, on two different lines for redundency, on multiple different channels for product partitioning, and also different types of data may be transmitted.
#
# In this case we have data on two lines:
# [A, B]
# From 5 different channels:
# [310, 311, 312, 313, 314]
# And four different types of data were sent:
# [Incr, MboS, Meta, Snap]
#
# We always follow the same naming scheme for each exchange we capture:
#    <feed_name>_<data_type>_<channel>_<line>_<YYYYMMDD>.pcap.gz
#
# As we don't want to miss anything from an exchange, we need some way to verify all of the different combinations have been captured.
# For this question, we would like you to write a python script that prints the names of any file that is found to be missing from the list below. In addition, if there are any extra files in this list knowing about them would be needed too.


# List we would like to check for missing/unexpected files.
@pytest.fixture
def ms_fixture():
    return ['cme_globex30_Incr_310_A_20190212.pcap.gz',
        'cme_globex30_Incr_310_B_20190212.pcap.gz',
        'cme_globex30_Incr_311_A_20190212.pcap.gz',
        'cme_globex30_Incr_311_B_20190212.pcap.gz',
        'cme_globex30_Incr_312_A_20190212.pcap.gz',
        'cme_globex30_Incr_312_B_20190212.pcap.gz',
        'cme_globex30_Incr_313_A_20190212.pcap.gz',
        'cme_globex30_Incr_313_B_20190212.pcap.gz',
        'cme_globex30_Incr_314_A_20190212.pcap.gz',
        'cme_globex30_Incr_314_B_20190212.pcap.gz',
        'cme_globex30_MboS_310_A_20190212.pcap.gz',
        'cme_globex30_MboS_310_B_20190212.pcap.gz',
        'cme_globex30_MboS_311_A_20190212.pcap.gz',
        'cme_globex30_MboS_311_B_20190212.pcap.gz',
        'cme_globex30_MboS_312_A_20190212.pcap.gz',
        'cme_globex30_MboS_312_B_20190212.pcap.gz',
        'cme_globex30_MboS_313_A_20190212.pcap.gz',
        'cme_globex30_MboS_313_B_20190212.pcap.gz',
        'cme_globex30_MboS_314_A_20190212.pcap.gz',
        'cme_globex30_MboS_314_B_20190212.pcap.gz',
        'cme_globex30_Meta_310_A_20190212.pcap.gz',
        'cme_globex30_Meta_310_B_20190212.pcap.gz',
        'cme_globex30_Meta_311_A_20190212.pcap.gz',
        'cme_globex30_Meta_311_B_20190212.pcap.gz',
        'cme_globex30_Meta_312_A_20190212.pcap.gz',
        'cme_globex30_Meta_312_B_20190212.pcap.gz',
        'cme_globex30_Meta_313_A_20190212.pcap.gz',
        'cme_globex30_Meta_313_B_20190212.pcap.gz',
        'cme_globex30_Meta_314_A_20190212.pcap.gz',
        'cme_globex30_Meta_314_B_20190212.pcap.gz',
        'cme_globex30_Snap_310_A_20190212.pcap.gz',
        'cme_globex30_Snap_310_B_20190212.pcap.gz',
        'cme_globex30_Snap_311_A_20190212.pcap.gz',
        'cme_globex30_Snap_311_B_20190212.pcap.gz',
        'cme_globex30_Snap_312_A_20190212.pcap.gz',
        'cme_globex30_Snap_312_B_20190212.pcap.gz',
        'cme_globex30_Snap_313_A_20190212.pcap.gz',
        'cme_globex30_Snap_313_B_20190212.pcap.gz',
        'cme_globex30_Snap_314_A_20190212.pcap.gz',
        'cme_globex30_Snap_314_B_20190212.pcap.gz']


# Answer here
# Takes in a list of the current files in the directory and the current date.
# Returns a list of missing files and a list of unexpected files.
def findMissingFiles(fileList, date):
    unexpected = []
    possible = []

    # no info on feed name is given so hardcode
    # no info on date is given so use dynamic date
    # could use itertools.product but this is clearer
    for fn in ["cme_globex30"]:
        for dt in ["Incr", "MboS", "Meta", "Snap"]:
            for ci in ["310", "311", "312", "313", "314"]:
                for lt in ["A", "B"]:
                    possible.append(f"{fn}_{dt}_{ci}_{lt}_{date}.pcap.gz")

    for file in fileList:
        try:
            possible.remove(file)
        except ValueError:
            unexpected.append(file)

    return possible, unexpected


def test_happy_path(ms_fixture):
    missing, unexpected = findMissingFiles(ms_fixture, "20190212")

    assert missing == []
    assert unexpected == []


def test_unexpected(ms_fixture):
    bad_channel = 'cme_globex30_Snap_999_B_20190212.pcap.gz'
    ms_fixture.append(bad_channel)
    bad_file = 'foobar.baz'
    ms_fixture.append(bad_file)

    missing, unexpected = findMissingFiles(ms_fixture, "20190212")

    assert missing == []
    assert len(unexpected) == 2
    assert bad_file in unexpected
    assert bad_channel in unexpected


def test_missing(ms_fixture):
    remove_file = 'cme_globex30_Snap_313_B_20190212.pcap.gz'
    remove_file2 = 'cme_globex30_MboS_313_A_20190212.pcap.gz'
    ms_fixture.remove(remove_file)
    ms_fixture.remove(remove_file2)

    missing, unexpected = findMissingFiles(ms_fixture, "20190212")

    assert len(missing) == 2
    assert remove_file in missing
    assert remove_file2 in missing
    assert unexpected == []


def test_dynamic_date():
    input_files = [
        'cme_globex30_Incr_310_A_20190215.pcap.gz',
        'cme_globex30_Incr_310_B_20190215.pcap.gz',
        'cme_globex30_Incr_311_A_20190215.pcap.gz',
        'cme_globex30_Incr_311_B_20190215.pcap.gz',
        'cme_globex30_Incr_312_A_20190215.pcap.gz',
        'cme_globex30_Incr_312_B_20190215.pcap.gz',
        'cme_globex30_Incr_313_A_20190215.pcap.gz',
        'cme_globex30_Incr_313_B_20190215.pcap.gz',
        'cme_globex30_Incr_314_A_20190215.pcap.gz',
        'cme_globex30_Incr_314_B_20190215.pcap.gz',
        'cme_globex30_MboS_310_A_20190215.pcap.gz',
        'cme_globex30_MboS_310_B_20190215.pcap.gz',
        'cme_globex30_MboS_311_A_20190215.pcap.gz',
        'cme_globex30_MboS_311_B_20190215.pcap.gz',
        'cme_globex30_MboS_312_A_20190215.pcap.gz',
        'cme_globex30_MboS_312_B_20190215.pcap.gz',
        'cme_globex30_MboS_313_A_20190215.pcap.gz',
        'cme_globex30_MboS_313_B_20190215.pcap.gz',
        'cme_globex30_MboS_314_A_20190215.pcap.gz',
        'cme_globex30_MboS_314_B_20190215.pcap.gz',
        'cme_globex30_Meta_310_A_20190215.pcap.gz',
        'cme_globex30_Meta_310_B_20190215.pcap.gz',
        'cme_globex30_Meta_311_A_20190215.pcap.gz',
        'cme_globex30_Meta_311_B_20190215.pcap.gz',
        'cme_globex30_Meta_312_A_20190215.pcap.gz',
        'cme_globex30_Meta_312_B_20190215.pcap.gz',
        'cme_globex30_Meta_313_A_20190215.pcap.gz',
        'cme_globex30_Meta_313_B_20190215.pcap.gz',
        'cme_globex30_Meta_314_A_20190215.pcap.gz',
        'cme_globex30_Meta_314_B_20190215.pcap.gz',
        'cme_globex30_Snap_310_A_20190215.pcap.gz',
        'cme_globex30_Snap_310_B_20190215.pcap.gz',
        'cme_globex30_Snap_311_A_20190215.pcap.gz',
        'cme_globex30_Snap_311_B_20190215.pcap.gz',
        'cme_globex30_Snap_312_A_20190215.pcap.gz',
        'cme_globex30_Snap_312_B_20190215.pcap.gz',
        'cme_globex30_Snap_313_A_20190215.pcap.gz',
        'cme_globex30_Snap_313_B_20190215.pcap.gz',
        'cme_globex30_Snap_314_A_20190215.pcap.gz',
        'cme_globex30_Snap_000_B_20190215.pcap.gz',
    ]

    missing, unexpected = findMissingFiles(input_files, "20190215")

    assert 'cme_globex30_Snap_314_B_20190215.pcap.gz' in missing
    assert 'cme_globex30_Snap_000_B_20190215.pcap.gz' in unexpected
