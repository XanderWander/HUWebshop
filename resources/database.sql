DROP TABLE IF EXISTS "brands" CASCADE;
CREATE TABLE "brands" (
  "id" SERIAL PRIMARY KEY,
  "name" varchar
);

DROP TABLE IF EXISTS "categories" CASCADE;
CREATE TABLE "categories" (
  "id" SERIAL PRIMARY KEY,
  "name" varchar
);

DROP TABLE IF EXISTS "colors" CASCADE;
CREATE TABLE "colors" (
  "id" SERIAL PRIMARY KEY,
  "name" varchar
);

DROP TABLE IF EXISTS "genders" CASCADE;
CREATE TABLE "genders" (
  "id" SERIAL PRIMARY KEY,
  "name" varchar
);

DROP TABLE IF EXISTS "products" CASCADE;
CREATE TABLE "products" (
  "id" SERIAL PRIMARY KEY,
  "_id" varchar,
  "deeplink" varchar,
  "description" varchar,
  "fast_mover" bool,
  "herhaalaankopen" bool,
  "name" varchar,
  "predict_out_of_stock_date" date,
  "price_discount" int,
  "price_mrsp" int,
  "selling_price" int,
  "brand_id" int,
  "category_id" int,
  "color_id" int,
  "gender_id" int
);

DROP TABLE IF EXISTS "images" CASCADE;
CREATE TABLE "images" (
  "id" SERIAL PRIMARY KEY,
  "link" varchar,
  "product_id" int
);

ALTER TABLE "products" ADD FOREIGN KEY ("brand_id") REFERENCES "brands" ("id");
ALTER TABLE "products" ADD FOREIGN KEY ("category_id") REFERENCES "categories" ("id");
ALTER TABLE "products" ADD FOREIGN KEY ("color_id") REFERENCES "colors" ("id");
ALTER TABLE "products" ADD FOREIGN KEY ("gender_id") REFERENCES "genders" ("id");
ALTER TABLE "images" ADD FOREIGN KEY ("product_id") REFERENCES "products" ("id");
