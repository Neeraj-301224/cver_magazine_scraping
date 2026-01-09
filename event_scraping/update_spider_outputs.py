"""
Helper script to update all run_*_spider.py files to use scraped_data folder.
This is a one-time script to update all spider runner files.
"""
import re
from pathlib import Path

def update_spider_file(file_path):
    """Update a single spider runner file to use scraped_data folder."""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check if already updated
    if 'scraped_data' in content:
        print(f"  ⏭️  {file_path.name} already updated")
        return False
    
    # Find the spider_name line and output_file line
    # Pattern 1: output_file = f"{spider_name}.json"
    pattern1 = r'output_file = f"\{spider_name\}\.json"'
    
    # Pattern 2: output_file = f"{spider_name}.json" (with quotes)
    pattern2 = r'output_file = f"\{spider_name\}\.json"'
    
    # More general pattern
    old_pattern = r'(output_file = f"\{spider_name\}\.json")'
    
    new_code = '''from pathlib import Path

    spider_name = "{spider_name}"
    # Save JSON files to scraped_data folder
    scraped_data_dir = Path(__file__).parent / "scraped_data"
    scraped_data_dir.mkdir(exist_ok=True)
    output_file = str(scraped_data_dir / f"{spider_name}.json")'''
    
    # Try to find and replace the section
    if 'if __name__ == "__main__":' in content:
        # Find the section after if __name__
        lines = content.split('\n')
        new_lines = []
        i = 0
        while i < len(lines):
            line = lines[i]
            if 'if __name__ == "__main__":' in line:
                new_lines.append(line)
                i += 1
                # Check if next lines have spider_name and output_file
                if i < len(lines) and 'spider_name =' in lines[i]:
                    # Extract spider_name
                    spider_name_match = re.search(r'spider_name = ["\'](\w+)["\']', lines[i])
                    if spider_name_match:
                        spider_name_val = spider_name_match.group(1)
                        new_lines.append(f'    from pathlib import Path')
                        new_lines.append('')
                        new_lines.append(f'    spider_name = "{spider_name_val}"')
                        new_lines.append('    # Save JSON files to scraped_data folder')
                        new_lines.append('    scraped_data_dir = Path(__file__).parent / "scraped_data"')
                        new_lines.append('    scraped_data_dir.mkdir(exist_ok=True)')
                        new_lines.append(f'    output_file = str(scraped_data_dir / f"{{spider_name}}.json")')
                        i += 1
                        # Skip old output_file line if exists
                        if i < len(lines) and 'output_file =' in lines[i]:
                            i += 1
                        continue
            new_lines.append(line)
            i += 1
        
        new_content = '\n'.join(new_lines)
        
        if new_content != content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            print(f"  ✅ Updated {file_path.name}")
            return True
    
    print(f"  ⚠️  Could not update {file_path.name} (pattern not found)")
    return False

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    spider_files = list(script_dir.glob('run_*_spider.py'))
    
    print(f"Found {len(spider_files)} spider runner files")
    updated = 0
    for file_path in spider_files:
        if update_spider_file(file_path):
            updated += 1
    
    print(f"\n✅ Updated {updated}/{len(spider_files)} files")

