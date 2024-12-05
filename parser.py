from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec

from bs4 import BeautifulSoup
from time import sleep
import json
import jsonmerge
import psycopg2
import numpy as np
import threading
import multiprocessing

import time


def get_register_items():
    registers = []
    # Connecting to database
    query = "SELECT registro FROM bulario_anvisa WHERE data IS NULL LIMIT 6000;"
    conn = get_connection()
    # Open a cursor to perform database operations
    cur = conn.cursor()
    # Execute a command: create datacamp_courses table
    cur.execute(query)
    rows = cur.fetchall()
    # Make the changes to the database persistent
    conn.commit()
    # Close cursor and communication with the database
    cur.close()
    conn.close()

    for row in rows:
        registers.append(row[0])

    return registers


def get_connection():

    # RETURN THE CONNECTION OBJECT
    return psycopg2.connect(
        database="bulario",
        user="mediassist",
        host="mediassist.cd0asy64s1dc.us-east-1.rds.amazonaws.com",
        password="wsdLx4fFXBZHEyU92D1S",
        port=5432,
    )


def dict_to_json(value: dict):
    return json.dumps(value)


def update_register_item(register, data):
    conn = get_connection()
    cur = conn.cursor()
    json_obj = dict_to_json(value=data)
    cur.execute(
        "UPDATE bulario_anvisa SET data = %s WHERE registro = %s", [json_obj, register]
    )

    conn.commit()
    cur.close()
    conn.close()


def process_drug_leaflet(registerNumber):
    service = Service(executable_path="")
    driver = webdriver.Chrome(service=service)

    driver.get("https://consultas.anvisa.gov.br/#/bulario/")
    # Wait to open page
    WebDriverWait(driver, 5).until(
        ec.presence_of_element_located((By.ID, "txtNumeroRegistro"))
    )

    input_element = driver.find_element(By.ID, "txtNumeroRegistro")
    input_element.send_keys(registerNumber)

    consultar_button = driver.find_element(By.CSS_SELECTOR, ".btn-primary")
    consultar_button.click()

    WebDriverWait(driver, 5).until(
        ec.presence_of_element_located(
            (By.XPATH, '//*[@id="containerTable"]/table/tbody/tr[2]/td[2]/a')
        )
    )

    drug_details = driver.find_element(
        By.XPATH, '//*[@id="containerTable"]/table/tbody/tr[2]/td[2]/a'
    )

    details_url = drug_details.get_attribute("href")
    driver.get(details_url)

    WebDriverWait(driver, 5).until(
        ec.presence_of_element_located(
            (By.XPATH, '//*[@id="containerTable"]/table/tbody/tr[2]/td[2]/a')
        )
    )

    drug_details = driver.find_element(
        By.XPATH, '//*[@id="containerTable"]/table/tbody/tr[2]/td[2]/a'
    )

    WebDriverWait(driver, 5).until(
        ec.presence_of_element_located(
            (By.XPATH, '//*[@class="btn btn-default no-print ng-scope"]')
        )
    )

    # drug_details_expand_button = driver.find_element(
    #     By.XPATH, '//*[@class="btn btn-default no-print ng-scope"]'
    # )

    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser", from_encoding="iso-8859-8")
    tables = soup.find_all("div", attrs={"class": "panel panel-default"})

    # Processing drug details
    product_details_table = tables[0]

    count = 0
    drug_details_json = {}
    for row in product_details_table.find_all("tr"):
        columns = row.find_all("td")
        if columns != [] and count == 0:
            current_row = {
                "product_name": columns[0].text.strip(),
                "brand_complement": columns[1].text.strip(),
                "process_number": columns[2].text.strip(),
            }
            drug_details_json = jsonmerge.merge(drug_details_json, current_row)

        if columns != [] and count == 1:
            current_row = {
                "regularization_number": columns[0].text.strip(),
                "regularization_date": columns[1].text.strip(),
                "regularization_due_date": columns[2].text.strip(),
            }
            drug_details_json = jsonmerge.merge(drug_details_json, current_row)

        if columns != [] and count == 2:
            current_row = {
                "regularization_company": columns[0].text.strip(),
                "cnpj": columns[1].text.strip(),
                "afe": columns[2].text.strip(),
            }
            drug_details_json = jsonmerge.merge(drug_details_json, current_row)

        if columns != [] and count == 3:
            current_row = {
                "active_ingredient": columns[0].text.strip(),
                "regularization_category": columns[1].text.strip(),
            }
            drug_details_json = jsonmerge.merge(drug_details_json, current_row)

        if columns != [] and count == 4:
            current_row = {
                "reference_drug": columns[0].text.strip(),
            }
            drug_details_json = jsonmerge.merge(drug_details_json, current_row)

        if columns != [] and count == 5:
            current_row = {
                "therapeutic_class": columns[0].text.strip(),
                "atc": columns[1].text.strip(),
            }
            drug_details_json = jsonmerge.merge(drug_details_json, current_row)

        if columns != [] and count == 6:
            current_row = ""
            if len(columns) == 2:
                current_row = {
                    "prioritization_type": columns[0].text.strip(),
                    "public_opinion": columns[1].text.strip(),
                    "matrix_process": "",
                }
            if len(columns) == 3:
                current_row = {
                    "prioritization_type": columns[0].text.strip(),
                    "public_opinion": columns[1].text.strip(),
                    "matrix_process": columns[2].text.strip(),
                }
            drug_details_json = jsonmerge.merge(drug_details_json, current_row)

        count = count + 1

    # print(json.dumps(drug_details_json, indent=4))
    # Processing drug presentations
    presentation_tables = tables[1]
    count = 0

    drug_presentations_array = presentation_tables.find_all("tbody")
    drug_presentations_array_json = []

    for drug_presentation in drug_presentations_array:
        current_presentation_index = drug_presentations_array.index(drug_presentation)

        if current_presentation_index != 0:
            presentation_table_item_rows = drug_presentation.find_all("tr")
            current_presentation_item = {}

            for row in presentation_table_item_rows:
                columns = row.find_all("td")
                presentation_table_item_row_index = presentation_table_item_rows.index(
                    row
                )

                if presentation_table_item_row_index == 0:
                    current_row = {
                        "id": columns[0].text.strip(),
                        "presentation": columns[1].text.strip(),
                        "register": columns[2].text.strip(),
                        "pharmaceutical_formula": columns[3].text.strip(),
                        "publication_date": columns[4].text.strip(),
                        "expiration_date": columns[5].text.strip(),
                    }
                    current_presentation_item = jsonmerge.merge(
                        current_presentation_item, current_row
                    )

                if presentation_table_item_row_index == 1:
                    current_row = {
                        "active_ingredient": columns[0].text.strip(),
                    }
                    current_presentation_item = jsonmerge.merge(
                        current_presentation_item, current_row
                    )
                if presentation_table_item_row_index == 2:
                    current_row = {
                        "complement": columns[0].text.strip(),
                    }
                    current_presentation_item = jsonmerge.merge(
                        current_presentation_item, current_row
                    )
                if presentation_table_item_row_index == 3:
                    current_row = {
                        "packaging": columns[0].text.strip(),
                    }
                    current_presentation_item = jsonmerge.merge(
                        current_presentation_item, current_row
                    )
                if presentation_table_item_row_index == 4:
                    current_row = {
                        "manufacturing_location": columns[0].text.strip(),
                    }
                    current_presentation_item = jsonmerge.merge(
                        current_presentation_item, current_row
                    )
                if presentation_table_item_row_index == 5:
                    current_row = {
                        "administration_via": columns[0].text.strip(),
                    }
                    current_presentation_item = jsonmerge.merge(
                        current_presentation_item, current_row
                    )
                if presentation_table_item_row_index == 6:
                    current_row = {
                        "preservation": columns[0].text.strip(),
                    }
                    current_presentation_item = jsonmerge.merge(
                        current_presentation_item, current_row
                    )
                if presentation_table_item_row_index == 7:
                    current_row = {
                        "prescription_restriction": columns[0].text.strip(),
                    }
                    current_presentation_item = jsonmerge.merge(
                        current_presentation_item, current_row
                    )
                if presentation_table_item_row_index == 8:
                    current_row = {
                        "usage_restriction": columns[0].text.strip(),
                    }
                    current_presentation_item = jsonmerge.merge(
                        current_presentation_item, current_row
                    )
                if presentation_table_item_row_index == 9:
                    current_row = {
                        "destination": columns[0].text.strip(),
                    }
                    current_presentation_item = jsonmerge.merge(
                        current_presentation_item, current_row
                    )
                if presentation_table_item_row_index == 10:
                    current_row = {
                        "tarja": columns[0].text.strip(),
                    }
                    current_presentation_item = jsonmerge.merge(
                        current_presentation_item, current_row
                    )
                if presentation_table_item_row_index == 11:
                    current_row = {
                        "fractional_presentation": columns[0].text.strip(),
                    }
                    current_presentation_item = jsonmerge.merge(
                        current_presentation_item, current_row
                    )
            drug_presentations_array_json.append(current_presentation_item)

    presentations_json = {"presentations": drug_presentations_array_json}
    drug_details_entire = jsonmerge.merge(drug_details_json, presentations_json)
    # print(json.dumps(drug_details_entire, indent=4))

    driver.quit()
    return drug_details_entire


def execute(items):
    for item in items:
        try:
            data = process_drug_leaflet(item)
            update_register_item(item, data)
        except Exception:
            print("Error when tried to update")


def main():
    items = get_register_items()
    arr = np.array(items)
    procs = 9  # Number of threads to create
    newarr = np.array_split(arr, procs)
    # Create a list of jobs and then iterate through
    # the number of threads appending each thread to
    # the job list
    jobs = []

    for i in range(0, procs):
        items_to_run = newarr[i]
        print(items_to_run)
        process = multiprocessing.Process(target=execute, args=(items_to_run,))
        jobs.append(process)

    # Start the threads (i.e. calculate the random number lists)
    for j in jobs:
        print("started")
        j.start()

    # Ensure all of the threads have finished
    for j in jobs:
        j.join()

    print("List processing complete.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
