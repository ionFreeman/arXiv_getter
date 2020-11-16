from unittest import TestCase

from extract_background import increment_roman_numeral


class Test(TestCase):
    def test_increment_roman_numeral(self):
        assert increment_roman_numeral("I") == "II"
        assert increment_roman_numeral("III") == "IV"
        assert increment_roman_numeral("IX") == "X"
        assert increment_roman_numeral("XCIX") == "C"
        assert increment_roman_numeral("CDXCIX") == "D"
        assert increment_roman_numeral("MCMLXXI") == "MCMLXXII"
        assert increment_roman_numeral("MMCMXCIX") == "MMM"

