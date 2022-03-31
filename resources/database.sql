DROP TABLE IF EXISTS "categories" CASCADE;
CREATE TABLE "categories" (
  "id" SERIAL PRIMARY KEY,
  "name" varchar
);

DROP TABLE IF EXISTS "sub_categories" CASCADE;
CREATE TABLE "sub_categories" (
  "id" SERIAL PRIMARY KEY,
  "name" varchar
);

DROP TABLE IF EXISTS "sub_sub_categories" CASCADE;
CREATE TABLE "sub_sub_categories" (
  "id" SERIAL PRIMARY KEY,
  "name" varchar
);

DROP TABLE IF EXISTS "brands" CASCADE;
CREATE TABLE "brands" (
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
  "id" varchar PRIMARY KEY,
  "fast_mover" bool,
  "herhaalaankopen" bool,
  "name" varchar,
  "predict_out_of_stock_date" date,
  "price_discount" int,
  "price_mrsp" int,
  "selling_price" int,
  "brand_id" int,
  "gender_id" int,
  "category_id" int,
  "sub_category_id" int,
  "sub_sub_category_id" int
);

DROP TABLE IF EXISTS "sessions" CASCADE;
CREATE TABLE "sessions" (
  "id" varchar PRIMARY KEY,
  "buid" varchar,
  "os" varchar,
  "has_sale" bool
);

DROP TABLE IF EXISTS "profiles" CASCADE;
CREATE TABLE "profiles" (
  "id" varchar PRIMARY KEY,
  "segment" varchar,
  "has_email" bool
);

DROP TABLE IF EXISTS "properties" CASCADE;
CREATE TABLE "properties" (
  "id" SERIAL PRIMARY KEY,
  "name" varchar,
  "value" varchar,
  "product_id" varchar
);

DROP TABLE IF EXISTS "buids" CASCADE;
CREATE TABLE "buids" (
  "id" SERIAL PRIMARY KEY,
  "buid" varchar,
  "profile_id" varchar
);

DROP TABLE IF EXISTS "viewed_before" CASCADE;
CREATE TABLE "viewed_before" (
  "id" SERIAL PRIMARY KEY,
  "product_id" varchar,
  "profile_id" varchar
);

DROP TABLE IF EXISTS "ordered_products" CASCADE;
CREATE TABLE "ordered_products" (
  "id" SERIAL PRIMARY KEY,
  "product_id" varchar,
  "session_id" varchar
);

DROP TABLE IF EXISTS "brand_preference" CASCADE;
CREATE TABLE "brand_preference" (
  "brand_id" int,
  "session_id" varchar,
  "views" int,
  "orders" int,
  PRIMARY KEY ("brand_id", "session_id")
);

DROP TABLE IF EXISTS "category_preference" CASCADE;
CREATE TABLE "category_preference" (
  "category_id" int,
  "session_id" varchar,
  "views" int,
  "orders" int,
  PRIMARY KEY ("category_id", "session_id")
);

DROP TABLE IF EXISTS "session_profiles" CASCADE;
CREATE TABLE "session_profiles" (
  "session_id" varchar,
  "profile_id" varchar,
  PRIMARY KEY ("session_id", "profile_id")
);

DROP TABLE IF EXISTS "order_count" CASCADE;
CREATE TABLE "order_count" (
  "product_id" varchar PRIMARY KEY,
  "order_count" int
);

ALTER TABLE "products" ADD FOREIGN KEY ("brand_id") REFERENCES "brands" ("id");
ALTER TABLE "products" ADD FOREIGN KEY ("gender_id") REFERENCES "genders" ("id");
ALTER TABLE "products" ADD FOREIGN KEY ("category_id") REFERENCES "categories" ("id");
ALTER TABLE "products" ADD FOREIGN KEY ("sub_category_id") REFERENCES "sub_categories" ("id");
ALTER TABLE "products" ADD FOREIGN KEY ("sub_sub_category_id") REFERENCES "sub_sub_categories" ("id");
ALTER TABLE "properties" ADD FOREIGN KEY ("product_id") REFERENCES "products" ("id");
ALTER TABLE "buids" ADD FOREIGN KEY ("profile_id") REFERENCES "profiles" ("id");
ALTER TABLE "viewed_before" ADD FOREIGN KEY ("product_id") REFERENCES "products" ("id");
ALTER TABLE "viewed_before" ADD FOREIGN KEY ("profile_id") REFERENCES "profiles" ("id");
ALTER TABLE "ordered_products" ADD FOREIGN KEY ("product_id") REFERENCES "products" ("id");
ALTER TABLE "ordered_products" ADD FOREIGN KEY ("session_id") REFERENCES "sessions" ("id");
ALTER TABLE "brand_preference" ADD FOREIGN KEY ("brand_id") REFERENCES "brands" ("id");
ALTER TABLE "brand_preference" ADD FOREIGN KEY ("session_id") REFERENCES "sessions" ("id");
ALTER TABLE "category_preference" ADD FOREIGN KEY ("category_id") REFERENCES "categories" ("id");
ALTER TABLE "category_preference" ADD FOREIGN KEY ("session_id") REFERENCES "sessions" ("id");
ALTER TABLE "session_profiles" ADD FOREIGN KEY ("session_id") REFERENCES "sessions" ("id");
ALTER TABLE "session_profiles" ADD FOREIGN KEY ("profile_id") REFERENCES "profiles" ("id");
ALTER TABLE "order_count" ADD FOREIGN KEY ("product_id") REFERENCES "products" ("id");
