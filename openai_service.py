import asyncio
import re
from pathlib import Path
from typing import List

import aiofiles
import openai
from tqdm import tqdm


class OpenAIService:
    def __init__(self, api_key: str, max_concurrent: int = 5):
        """Initialize OpenAI service with API key and max concurrent requests."""
        self.client = openai.AsyncOpenAI(api_key=api_key)
        self.max_concurrent = max_concurrent
        self.system_prompt = """I am writing search keywords for inspiration, art, design, photography, and other artistic fields. 
Please generate a list of search keywords based on the given topic, following the format below. 
Each keyword should be on a new line, and try to be as comprehensive as possible.

Example format:
list architecture
simple product design
japanese minimalism
scandinavian minimalism
monochromatic spaces"""

    async def generate_keywords(self, topic: str) -> List[str]:
        """Generate keywords for a given topic using GPT-4."""
        try:
            response = await self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": self.system_prompt},
                    {
                        "role": "user",
                        "content": f"Please generate search keywords for: {topic}",
                    },
                ],
                temperature=0.7,
                max_tokens=1000,
            )

            # Split the response into individual keywords and clean them
            keywords = [
                keyword.strip()
                for keyword in response.choices[0].message.content.split("\n")
                if keyword.strip()
            ]
            return keywords
        except Exception as e:
            print(f"Error generating keywords for {topic}: {str(e)}")
            return []

    async def save_keywords(self, topic: str, keywords: List[str], output_dir: Path):
        """Save keywords to a file named after the topic."""
        # Create a safe filename from the topic
        safe_filename = re.sub(r'[<>:"/\\|?*]', "_", topic)
        file_path = output_dir / f"{safe_filename}.txt"

        async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
            await f.write("\n".join(keywords))

    async def process_topic(self, topic: str, output_dir: Path, pbar: tqdm):
        """Process a single topic and save its keywords."""
        keywords = await self.generate_keywords(topic)
        if keywords:
            await self.save_keywords(topic, keywords, output_dir)
        pbar.update(1)

    async def process_file(self, file_path: str, output_dir: str):
        """Process a file line by line and generate keywords for each line concurrently."""
        input_path = Path(file_path)
        output_dir = Path(output_dir)

        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {file_path}")

        # Create output directory if it doesn't exist
        output_dir.mkdir(parents=True, exist_ok=True)

        # Read all lines from input file
        async with aiofiles.open(input_path, "r", encoding="utf-8") as f:
            content = await f.read()
            topics = [line.strip() for line in content.split("\n") if line.strip()]

        # Process topics concurrently with a progress bar
        with tqdm(total=len(topics), desc="Processing topics") as pbar:
            # Create semaphore to limit concurrent requests
            semaphore = asyncio.Semaphore(self.max_concurrent)

            async def process_with_semaphore(topic: str):
                async with semaphore:
                    await self.process_topic(topic, output_dir, pbar)

            # Create tasks for all topics
            tasks = [process_with_semaphore(topic) for topic in topics]

            # Wait for all tasks to complete
            await asyncio.gather(*tasks)


async def main():
    """Main function to demonstrate usage."""
    # Replace with your actual API key
    api_key = "xxx"

    service = OpenAIService(api_key, max_concurrent=5)

    # Example usage
    input_file = "input_topics.txt"
    output_dir = "output_keywords"

    await service.process_file(input_file, output_dir)


if __name__ == "__main__":
    asyncio.run(main())
