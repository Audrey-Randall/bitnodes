# 1st element of the tuple is the first version that is flawed
# 2nd element is patched version (not vulnerable anymore)
# if only one element in tuple, it means that prior versions are flawed
vulnerable_client_versions = {
    "CVE-2016-10724": [
        (["/Satoshi:"], ("0.13.0",))
    ],
    "CVE-2016-10725": [
        (["/Satoshi:"], ("0.13.0",))
    ],
    "CVE-2017-18350": [
        (["/Satoshi:"], ("0.15.1",))
    ],
    "CVE-2018-17144": [
        (["/Satoshi:"], ("0.14.0", "0.14.3")),
        (["/Satoshi:"], ("0.15.0", "0.15.2")),
        (["/Satoshi:"], ("0.16.0", "0.16.3")),
        (["/Satoshi:", "/Knots"], ("0.14.0", "0.16.3")),
    ],
    "CVE-2018-20586": [
        (["/Satoshi:"], ("0.17.1",))
    ],
    "CVE-2018-20587": [
        (["/Satoshi:"], ("0.12.0", "0.17.1")),
        (["/Satoshi:", "/Knots"], ("0.12.0", "0.17.1")),
    ],
}
