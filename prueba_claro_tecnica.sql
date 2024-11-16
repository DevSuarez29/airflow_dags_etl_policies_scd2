-- Crear la tabla "bob" con claves foráneas
CREATE TABLE "bob" (
  "holder_id" VARCHAR(100) NOT NULL,
  "policy_id" VARCHAR(100) NOT NULL,
  "days_of_payment_delay" INT,
  "commission_amount" FLOAT,
  "members" INT,
  "valid_from" TIMESTAMP,
  "valid_to" TIMESTAMP,
  "is_current" BOOLEAN DEFAULT true,
  "updated_at" TIMESTAMP,
  "created_at" TIMESTAMP,
  FOREIGN KEY ("holder_id") REFERENCES "agents" ("holder_id"),
  FOREIGN KEY ("policy_id") REFERENCES "policies" ("policy_id")
);

-- Crear la tabla "agents"
CREATE TABLE "agents" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "holder_id" VARCHAR(100) NOT NULL,
  "holder_name" VARCHAR(100),
  "holder_age" INT,
  "holder_phone_number" VARCHAR(20),
  "holder_incomes" FLOAT,
  "holder_address" VARCHAR(255),
  "valid_from" TIMESTAMP,
  "valid_to" TIMESTAMP,
  "is_current" BOOLEAN DEFAULT true,
  "updated_at" TIMESTAMP,
  "created_at" TIMESTAMP
);
 
-- Crear la tabla "policies"
CREATE TABLE "policies" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "policy_id" VARCHAR(100) NOT NULL,
  "start_date" TIMESTAMP,
  "end_date" TIMESTAMP,
  "carrier_name" VARCHAR(100),
  "valid_from" TIMESTAMP,
  "valid_to" TIMESTAMP,
  "is_current" BOOLEAN DEFAULT true,
  "updated_at" TIMESTAMP,
  "created_at" TIMESTAMP
);
