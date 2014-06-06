import unittest
import datetime
import sys
sys.path.insert(0, "../")
import common

class TestObservation(unittest.TestCase):

    def test_send_email(self):
        ''' See if we could potentially spam myself, this will not actually
        do anything as we aren't running a twisted reactor '''
        self.assertTrue( common.email_error("MyException", "test_common.py"))

    def test_should_email(self):
        """ Test should_email() logic """
        common.email_timestamps = []
        self.assertTrue(common.should_email())

        for mi in range(60):
            common.email_timestamps.append( datetime.datetime.utcnow() -
                                            datetime.timedelta(minutes=mi))
        self.assertTrue(not common.should_email())

        common.email_timestamps = []
        for hr in range(11):
            common.email_timestamps.append( datetime.datetime.utcnow() -
                                            datetime.timedelta(hours=hr))
        self.assertTrue(common.should_email())


if __name__ == '__main__':
    unittest.main()
