ENTITIES := customer order product sales_channel carrier category promotion address brand vendor vendor_address customer_address product_variant warehouse inventory inventory_movement order_item order_address order_status_history payment refund shipment shipment_item returns return_item order_item_discount product_review cart cart_item customer_event

bronze:
	python -m src.bronze.$(ENTITY)

silver:
	python -m src.silver.$(ENTITY)

bronze-all:
	@for entity in $(ENTITIES); do \
		echo ">>> Running bronze pipeline for $$entity..."; \
		python -m src.bronze.$$entity; \
	done

silver-all:
	@for entity in $(ENTITIES); do \
		echo ">>> Running silver pipeline for $$entity..."; \
		python -m src.silver.$$entity; \
	done

pipeline:
	python run_pipeline.py $(ARGS)

pipeline-parallel:
	python run_pipeline.py --parallel 4 $(ARGS)

dbt-deps:
	cd dbt_retail && dbt deps

dbt-run:
	cd dbt_retail && dbt run

dbt-test:
	cd dbt_retail && dbt test

dbt-docs-generate:
	cd dbt_retail && dbt docs generate

dbt-docs-serve:
	cd dbt_retail && dbt docs serve --port 8080

setup:
	pip install -r requirements.txt

lint:
	ruff check src/

clean:
	rm -rf src/**/__pycache__ .pytest_cache
