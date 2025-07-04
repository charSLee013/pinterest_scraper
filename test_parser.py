import unittest
import json
from parser import enrich_pin_data_from_json, extract_image_urls_from_src, find_largest_image_url
import config

# Mock config for testing purposes if needed
class MockConfig:
    ORIGINAL_SIZE_MARKER = "originals"
    IMAGE_SIZES = [75, 150, 236, 474, 564, 736]

config = MockConfig()

class TestParser(unittest.TestCase):

    def test_enrich_pin_data_from_json_images_key(self):
        # Test case with 'images' key
        pin_data = {}
        json_data = {
            "images": {
                "236x": {"url": "https://example.com/236x/image.jpg"},
                "orig": {"url": "https://example.com/originals/image.jpg"},
                "564x": {"url": "https://example.com/564x/image.jpg"}
            },
            "description": "Test Description",
            "creator": {"full_name": "Test Creator", "username": "testuser"}
        }
        result = enrich_pin_data_from_json(pin_data, json_data)

        self.assertIn("original", result["image_urls"])
        self.assertIn("236", result["image_urls"])
        self.assertIn("564", result["image_urls"])
        self.assertEqual(result["description"], "Test Description")
        self.assertEqual(result["creator"]["name"], "Test Creator")
        self.assertEqual(result["largest_image_url"], "https://example.com/originals/image.jpg")

    def test_enrich_pin_data_from_json_contenturl_image_url(self):
        # Test case with 'contentUrl', 'image', 'url' keys
        pin_data = {}
        json_data = {
            "contentUrl": "https://example.com/pin/123/image_content.jpg",
            "image": {"url": "https://example.com/pin/123/image_obj.jpg"},
            "url": "https://example.com/pin/123/image_generic.jpg",
            "description": "Another Description",
            "title": "Test Title"
        }
        result = enrich_pin_data_from_json(pin_data, json_data)

        # The extract_image_urls_from_src will prioritize finding sizes
        # and then add the original. So, we expect the latest one processed.
        # Based on the code, it iterates 'contentUrl', 'image', 'url'.
        # The 'url' key should be the last one processed if all exist.
        self.assertIn("original", result["image_urls"])
        self.assertIn("Test Title", result["title"])
        self.assertEqual(result["description"], "Another Description")
        # Since extract_image_urls_from_src is called, the original size marker
        # should be added. We're testing the 'enrich' function's ability to use it.
        # The exact URL depends on the internal logic of extract_image_urls_from_src
        # if the URL doesn't contain size markers.
        # For simplicity, we check if original is present and not empty.
        self.assertTrue(result["image_urls"]["original"])


    def test_enrich_pin_data_from_json_thumbnailurl(self):
        # Test case with 'thumbnailUrl' as list and string
        pin_data = {}
        json_data_list = {
            "thumbnailUrl": [
                "https://example.com/thumbs/100x/thumb1.jpg",
                "https://example.com/thumbs/200x/thumb2.jpg"
            ],
            "description": "Thumbnail List"
        }
        result_list = enrich_pin_data_from_json(pin_data, json_data_list)
        self.assertIn("100", result_list["image_urls"])
        self.assertIn("200", result_list["image_urls"])
        self.assertEqual(result_list["description"], "Thumbnail List")

        pin_data = {}
        json_data_string = {
            "thumbnailUrl": "https://example.com/thumbs/300x/thumb3.jpg",
            "description": "Thumbnail String"
        }
        result_string = enrich_pin_data_from_json(pin_data, json_data_string)
        self.assertIn("300", result_string["image_urls"])
        self.assertEqual(result_string["description"], "Thumbnail String")

    def test_enrich_pin_data_from_json_no_image_data(self):
        # Test case with no image data
        pin_data = {"id": "123"}
        json_data = {
            "description": "No Image",
            "creator": {"full_name": "No Image Creator"}
        }
        result = enrich_pin_data_from_json(pin_data, json_data)
        self.assertEqual(result["id"], "123")
        self.assertEqual(result["description"], "No Image")
        self.assertEqual(result["image_urls"], {})
        self.assertEqual(result["largest_image_url"], "")

    def test_enrich_pin_data_from_json_overwrite_pin_data(self):
        # Test case to ensure json_data overwrites pin_data if present
        pin_data = {
            "id": "old_id",
            "description": "old description",
            "image_urls": {"old": "old_url"},
            "largest_image_url": "old_largest",
            "creator": {"name": "Old Creator"}
        }
        json_data = {
            "id": "new_id",
            "description": "new description",
            "images": {"orig": {"url": "https://new.com/original.jpg"}},
            "creator": {"full_name": "New Creator"}
        }
        result = enrich_pin_data_from_json(pin_data, json_data)
        self.assertEqual(result["id"], "new_id")
        self.assertEqual(result["description"], "new description")
        self.assertEqual(result["image_urls"]["original"], "https://new.com/original.jpg")
        self.assertEqual(result["largest_image_url"], "https://new.com/original.jpg")
        self.assertEqual(result["creator"]["name"], "New Creator")

    def test_enrich_pin_data_from_json_nested_creator(self):
        pin_data = {}
        json_data = {
            "description": "Nested Creator",
            "creator": {
                "name": "Nested User",
                "url": "https://pinterest.com/nesteduser",
                "id": "12345",
                "follower_count": 100,
                "image_medium_url": "https://example.com/avatar.jpg"
            }
        }
        result = enrich_pin_data_from_json(pin_data, json_data)
        self.assertEqual(result["creator"]["name"], "Nested User")
        self.assertEqual(result["creator"]["id"], "12345")
        self.assertEqual(result["creator"]["follower_count"], 100)
        self.assertEqual(result["creator"]["avatar_url"], "https://example.com/avatar.jpg")

    def test_enrich_pin_data_from_json_stats(self):
        pin_data = {}
        json_data = {
            "description": "Stats Test",
            "like_count": 10,
            "repin_count": 20,
            "comment_count": 5
        }
        result = enrich_pin_data_from_json(pin_data, json_data)
        self.assertEqual(result["stats"]["likes"], 10)
        self.assertEqual(result["stats"]["saves"], 20)
        self.assertEqual(result["stats"]["comments"], 5)

    def test_enrich_pin_data_from_json_board(self):
        pin_data = {}
        json_data = {
            "description": "Board Test",
            "board": {
                "id": "board123",
                "name": "My Board / Subcategory",
                "url": "/testboard/"
            }
        }
        result = enrich_pin_data_from_json(pin_data, json_data)
        self.assertEqual(result["board"]["id"], "board123")
        self.assertEqual(result["board"]["name"], "My Board / Subcategory")
        self.assertEqual(result["board"]["url"], "https://www.pinterest.com/testboard/")
        self.assertEqual(result["categories"], ["My Board", "Subcategory"])

    def test_enrich_pin_data_from_json_missing_keys(self):
        # Test case with missing optional keys
        pin_data = {}
        json_data = {
            "id": "missing_keys_id",
            "description": "Missing Keys Test"
        }
        result = enrich_pin_data_from_json(pin_data, json_data)
        self.assertEqual(result["id"], "missing_keys_id")
        self.assertEqual(result["description"], "Missing Keys Test")
        self.assertNotIn("creator", result)
        self.assertNotIn("stats", result)
        self.assertNotIn("board", result)
        self.assertEqual(result["image_urls"], {})
        self.assertEqual(result["largest_image_url"], "")

if __name__ == '__main__':
    unittest.main()
