import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup

# Đường dẫn đến thư mục lưu trữ dữ liệu đã crawl
output_dir = "data_crawled"

# Tạo thư mục nếu chưa tồn tại
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Khởi tạo EdgeDriver mà không có tùy chọn nào.
def get_driver():
    edge_driver_path = "C:/EdgeDriver/msedgedriver.exe"  # Thay đổi đường dẫn nếu cần
    service = EdgeService(executable_path=edge_driver_path)
    return webdriver.Edge(service=service)

def wait_and_click(driver, by, value, timeout=10):
    try:
        # Đợi phần tử trở nên có thể nhấp được
        element = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((by, value))
        )
        # Cuộn đến phần tử
        driver.execute_script("arguments[0].scrollIntoView(true);", element)
        time.sleep(1)  # Thêm thời gian chờ trước khi nhấp
        element.click()
        return True
    except Exception as e:
        print(f"Đã xảy ra lỗi khi nhấp vào phần tử: {e}")
        return False

def extract_and_save_car_data(page_number):
    driver = get_driver()
    max_retries = 3
    retry_count = 0

    try:
        while retry_count < max_retries:
            try:
                print(f"Đang lấy dữ liệu từ trang {page_number}...")
                driver.get(f'https://www.truecar.com/used-cars-for-sale/listings/?page={page_number}')

                # Đợi cho trang web tải xong và phần tử mong đợi xuất hiện
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'a[data-test="vehicleCardLink"]'))
                )

                html_content = driver.page_source
                soup = BeautifulSoup(html_content, 'html.parser')

                # Tìm tất cả các mục xe ô tô
                car_hrefs = []
                car_items = soup.find_all("a", {"data-test": "vehicleCardLink"})

                for car in car_items:
                    car_hrefs.append(car['href'])

                print(f"Tìm thấy {len(car_hrefs)} hrefs trên trang {page_number}.")

                for index, href in enumerate(car_hrefs):
                    print(f"Đang xử lý href {index + 1}/{len(car_hrefs)}: {href}")
                    driver.get(f"https://www.truecar.com{href}")
                    time.sleep(random.uniform(3, 7))  # Dừng lại ngẫu nhiên từ 3 đến 7 giây

                    car_html_content = driver.page_source
                    car_soup = BeautifulSoup(car_html_content, 'html.parser')

                    # Trích xuất tiêu đề
                    title_element = car_soup.find("div", {"data-test": "marketplaceVdpHeader"})
                    title = title_element.get_text(strip=True) if title_element else "No Title"

                    price_cash_element = car_soup.select_one('div[data-test="unifiedPricingInfoPrice"]')
                    if not price_cash_element:
                        price_cash_element = car_soup.find('div', {"data-test": "unifiedPricingInfoPrice"})

                    price_cash = price_cash_element.get_text(strip=True) if price_cash_element else "No Cash Price"

                    # Nhấp vào nút Finance để cập nhật giá Finance
                    if not wait_and_click(driver, By.XPATH, '//label[@data-test="vdpPricingBlockLoanToggle"]'):
                        print(f"Lỗi khi nhấp vào nút Finance cho href {href}")

                    # Tạo lại BeautifulSoup sau khi nhấp nút
                    car_html_content = driver.page_source
                    car_soup = BeautifulSoup(car_html_content, 'html.parser')

                    # Trích xuất giá tài chính và thông tin chi tiết
                    finance_price_element = car_soup.select_one('div[data-test="unifiedPricingInfoPrice"]')
                    finance_price = finance_price_element.get_text(strip=True) if finance_price_element else "No Finance Price"

                    finance_details_element = car_soup.select_one('div[data-test="unifiedPricingInfoDisclaimer"]')
                    finance_details = finance_details_element.get_text(strip=True) if finance_details_element else "No Finance Details"

                    # Trích xuất các thông tin chi tiết khác
                    overview_section = car_soup.find('div', {'data-test': 'vehicleOverviewSection'})
                    if overview_section:
                        overview_items = overview_section.find_all('div', class_='flex items-center')
                        overview_info = {}
                        for item in overview_items:
                            text = item.get_text(strip=True)
                            if 'Exterior:' in text:
                                overview_info['Exterior'] = text.replace('Exterior:', '').strip()
                            elif 'Interior:' in text:
                                overview_info['Interior'] = text.replace('Interior:', '').strip()
                            elif 'miles' in text:
                                overview_info['Mileage'] = text.strip()
                            elif 'Fuel Type:' in text:
                                overview_info['Fuel Type'] = text.replace('Fuel Type:', '').strip()
                            elif 'city /' in text:
                                overview_info['MPG'] = text.strip()
                            elif 'Transmission' in text:
                                overview_info['Transmission'] = text.strip()
                            elif 'Drivetrain:' in text:
                                overview_info['Drivetrain'] = text.replace('Drivetrain:', '').strip()
                            elif 'engine' in text:
                                overview_info['Engine'] = text.strip()
                            elif 'Location:' in text:
                                overview_info['Location'] = text.replace('Location:', '').strip()
                            elif 'Listed' in text:
                                overview_info['Listed Since'] = text.strip()
                            elif 'VIN:' in text:
                                overview_info['VIN'] = text.replace('VIN:', '').strip()
                            elif 'Stock Number:' in text:
                                overview_info['Stock Number'] = text.replace('Stock Number:', '').strip()
                    else:
                        overview_info = {
                            'Exterior': 'N/A', 'Interior': 'N/A', 'Mileage': 'N/A', 'Fuel Type': 'N/A',
                            'MPG': 'N/A', 'Transmission': 'N/A', 'Drivetrain': 'N/A', 'Engine': 'N/A',
                            'Location': 'N/A', 'Listed Since': 'N/A', 'VIN': 'N/A', 'Stock Number': 'N/A'
                        }

                    # Nhấp vào nút "View more details"
                    if not wait_and_click(driver, By.XPATH, '//button/span[text()="View more details"]/..'):
                        print(f"Lỗi khi nhấp vào nút 'View more details' cho href {href}")

                    # Nhấp vào nút "See all features"
                    if not wait_and_click(driver, By.XPATH, '//button/span[text()="See all features"]/..'):
                        print(f"Lỗi khi nhấp vào nút 'See all features' cho href {href}")
                        feature_list = []
                    else:
                        # Tạo lại BeautifulSoup sau khi nhấp nút
                        car_html_content = driver.page_source
                        car_soup = BeautifulSoup(car_html_content, 'html.parser')

                        # Trích xuất các đặc điểm từ modal
                        features = car_soup.select('div.modal-body ul li')
                        feature_list = [feature.get_text(strip=True) for feature in features]

                    # Tạo thư mục con cho số trang nếu nó chưa tồn tại.
                    sub_dir = os.path.join(output_dir, str(page_number))
                    if not os.path.exists(sub_dir):
                        os.makedirs(sub_dir)

                    # Định nghĩa tên tệp
                    file_name = os.path.join(sub_dir, f"{index + 1}.txt")

                    # Chuẩn bị nội dung tệp
                    file_content = (
                        f"Title: {title}\n"
                        f"Cash Price: {price_cash}\n"
                        f"Finance Price: {finance_price}\n"
                        f"Finance Details: {finance_details}\n"
                        + "\n".join(f"{key}: {value}" for key, value in overview_info.items()) + "\n"
                        f"Features:\n" + "\n".join(feature_list) + "\n"
                    )

                    # Write file content to file
                    with open(file_name, "w", encoding="utf-8") as file:
                        file.write(file_content)

                    print(f"Đã lưu dữ liệu từ bài viết {href} vào tệp {file_name}")

                break  # Thoát khỏi vòng lặp nếu thành công

            except ConnectionResetError as e:
                retry_count += 1
                print(f"ConnectionResetError occurred on page {page_number}. Retrying ({retry_count}/{max_retries})...")
                time.sleep(random.uniform(2, 5))

            except Exception as e:
                print(f"Có lỗi xảy ra khi xử lý trang {page_number}: {str(e)}")
                break
    finally:
        driver.quit()
# Số trang bạn muốn thu thập
total_pages = 1

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(extract_and_save_car_data, page_number) for page_number in range(1, total_pages + 1)]
    for future in as_completed(futures):
        try:
            future.result()
        except Exception as e:
            print(f"Có lỗi xảy ra khi xử lý một trang: {str(e)}")

print("Quá trình thu thập dữ liệu đã hoàn tất.")