


SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;


COMMENT ON SCHEMA "public" IS 'standard public schema';



CREATE EXTENSION IF NOT EXISTS "pg_stat_statements" WITH SCHEMA "extensions";






CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA "extensions";






CREATE EXTENSION IF NOT EXISTS "supabase_vault" WITH SCHEMA "vault";






CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA "extensions";






CREATE OR REPLACE FUNCTION "public"."get_trending_cars"("limit_count" integer DEFAULT 5) RETURNS TABLE("brand" "text", "model" "text", "display_name" "text", "clicks" bigint, "body_type" "text", "fuel_type" "text", "transmission" "text")
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  RETURN QUERY
  SELECT
    l.brand,
    l.model,
    (l.year || ' ' || l.brand || ' ' || l.model)::text as display_name,
    COALESCE(SUM(lv.view_count), 0) as clicks,
    l.body_type,
    l.fuel_type,
    l.transmission
  FROM public.listings l
  LEFT JOIN public.listing_views lv ON l.id = lv.listing_id
  WHERE l.status = 'approved'
  GROUP BY l.id, l.brand, l.model, l.year, l.body_type, l.fuel_type, l.transmission
  ORDER BY clicks DESC
  LIMIT limit_count;
END;
$$;


ALTER FUNCTION "public"."get_trending_cars"("limit_count" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."import_listing"("p_brand" "text", "p_model" "text", "p_year" integer, "p_price" integer, "p_original_price" integer DEFAULT NULL::integer, "p_color" "text" DEFAULT NULL::"text", "p_mileage" integer DEFAULT NULL::integer, "p_body_type" "text" DEFAULT NULL::"text", "p_fuel_type" "text" DEFAULT NULL::"text", "p_transmission" "text" DEFAULT NULL::"text", "p_location" "text" DEFAULT 'Metro Manila, Philippines'::"text", "p_description" "text" DEFAULT NULL::"text", "p_images" "text"[] DEFAULT NULL::"text"[], "p_user_id" "uuid" DEFAULT NULL::"uuid") RETURNS "uuid"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  v_slug text;
  v_id uuid;
BEGIN
  -- Generate slug
  v_slug := lower(p_brand || '-' || p_model || '-' || p_year || '-' || p_price);
  v_slug := replace(v_slug, ' ', '-');
  
  -- Insert or update
  INSERT INTO public.listings (
    brand, model, year, price, original_price, color, mileage,
    body_type, fuel_type, transmission, location, description, images, user_id, status
  ) VALUES (
    p_brand, p_model, p_year, p_price, p_original_price, p_color, p_mileage,
    p_body_type, p_fuel_type, p_transmission, p_location, p_description, p_images, p_user_id, 'approved'
  )
  ON CONFLICT (slug) 
  DO UPDATE SET
    price = EXCLUDED.price,
    original_price = EXCLUDED.original_price,
    color = EXCLUDED.color,
    mileage = EXCLUDED.mileage,
    updated_at = now()
  RETURNING id INTO v_id;
  
  RETURN v_id;
END;
$$;


ALTER FUNCTION "public"."import_listing"("p_brand" "text", "p_model" "text", "p_year" integer, "p_price" integer, "p_original_price" integer, "p_color" "text", "p_mileage" integer, "p_body_type" "text", "p_fuel_type" "text", "p_transmission" "text", "p_location" "text", "p_description" "text", "p_images" "text"[], "p_user_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."track_car_view"("car_id" "uuid") RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  INSERT INTO public.listing_views (listing_id, view_date, view_count)
  VALUES (car_id, CURRENT_DATE, 1)
  ON CONFLICT (listing_id, view_date)
  DO UPDATE SET view_count = listing_views.view_count + 1;
END;
$$;


ALTER FUNCTION "public"."track_car_view"("car_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."update_updated_at_column"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."update_updated_at_column"() OWNER TO "postgres";

SET default_tablespace = '';

SET default_table_access_method = "heap";


CREATE TABLE IF NOT EXISTS "public"."listing_views" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "listing_id" "uuid" NOT NULL,
    "view_date" "date" DEFAULT CURRENT_DATE NOT NULL,
    "view_count" integer DEFAULT 1
);


ALTER TABLE "public"."listing_views" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."listings" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "slug" "text" NOT NULL,
    "brand" "text" NOT NULL,
    "model" "text" NOT NULL,
    "year" integer NOT NULL,
    "price" integer NOT NULL,
    "original_price" integer,
    "body_type" "text",
    "fuel_type" "text",
    "transmission" "text",
    "location" "text" DEFAULT 'Metro Manila, Philippines'::"text",
    "description" "text",
    "images" "text"[],
    "status" "text" DEFAULT 'approved'::"text",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "updated_at" timestamp with time zone DEFAULT "now"(),
    "user_id" "uuid",
    "color" "text",
    "mileage" integer,
    "edit_proposal" "jsonb",
    CONSTRAINT "listings_status_check" CHECK (("status" = ANY (ARRAY['pending'::"text", 'approved'::"text", 'rejected'::"text"])))
);


ALTER TABLE "public"."listings" OWNER TO "postgres";


COMMENT ON COLUMN "public"."listings"."edit_proposal" IS 'Stores proposed changes when a listing is edited and reverted to pending status';



CREATE TABLE IF NOT EXISTS "public"."users" (
    "id" "uuid" NOT NULL,
    "name" "text",
    "phone" "text" NOT NULL,
    "is_admin" boolean DEFAULT false,
    "created_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."users" OWNER TO "postgres";


ALTER TABLE ONLY "public"."listing_views"
    ADD CONSTRAINT "listing_views_listing_id_view_date_key" UNIQUE ("listing_id", "view_date");



ALTER TABLE ONLY "public"."listing_views"
    ADD CONSTRAINT "listing_views_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."listings"
    ADD CONSTRAINT "listings_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."listings"
    ADD CONSTRAINT "listings_slug_key" UNIQUE ("slug");



ALTER TABLE ONLY "public"."users"
    ADD CONSTRAINT "users_pkey" PRIMARY KEY ("id");



CREATE INDEX "idx_listings_approved_created" ON "public"."listings" USING "btree" ("created_at" DESC) WHERE ("status" = 'approved'::"text");



CREATE INDEX "idx_listings_body_type" ON "public"."listings" USING "btree" ("body_type");



CREATE INDEX "idx_listings_brand" ON "public"."listings" USING "btree" ("brand");



CREATE INDEX "idx_listings_brand_model" ON "public"."listings" USING "btree" ("brand", "model");



CREATE INDEX "idx_listings_fuel_type" ON "public"."listings" USING "btree" ("fuel_type");



CREATE INDEX "idx_listings_price" ON "public"."listings" USING "btree" ("price");



CREATE INDEX "idx_listings_slug" ON "public"."listings" USING "btree" ("slug");



CREATE INDEX "idx_listings_status" ON "public"."listings" USING "btree" ("status");



CREATE INDEX "idx_listings_transmission" ON "public"."listings" USING "btree" ("transmission");



CREATE INDEX "idx_listings_user_id" ON "public"."listings" USING "btree" ("user_id");



CREATE INDEX "idx_listings_year" ON "public"."listings" USING "btree" ("year");



CREATE OR REPLACE TRIGGER "update_listings_updated_at" BEFORE UPDATE ON "public"."listings" FOR EACH ROW EXECUTE FUNCTION "public"."update_updated_at_column"();



ALTER TABLE ONLY "public"."listing_views"
    ADD CONSTRAINT "listing_views_listing_id_fkey" FOREIGN KEY ("listing_id") REFERENCES "public"."listings"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."listings"
    ADD CONSTRAINT "listings_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "auth"."users"("id");



ALTER TABLE ONLY "public"."users"
    ADD CONSTRAINT "users_id_fkey" FOREIGN KEY ("id") REFERENCES "auth"."users"("id");



CREATE POLICY "Admins can view all listings" ON "public"."listings" FOR SELECT USING ((EXISTS ( SELECT 1
   FROM "public"."users"
  WHERE ((("users"."id")::"text" = ("auth"."uid"())::"text") AND ("users"."is_admin" = true)))));



CREATE POLICY "Authenticated users can create own listings" ON "public"."listings" FOR INSERT WITH CHECK ((("auth"."uid"())::"text" = ("user_id")::"text"));



CREATE POLICY "Public can view approved listings" ON "public"."listings" FOR SELECT USING (("status" = 'approved'::"text"));



CREATE POLICY "Users can delete own listings" ON "public"."listings" FOR DELETE USING ((("auth"."uid"())::"text" = ("user_id")::"text"));



CREATE POLICY "Users can update own listings" ON "public"."listings" FOR UPDATE USING ((("auth"."uid"())::"text" = ("user_id")::"text")) WITH CHECK ((("auth"."uid"())::"text" = ("user_id")::"text"));



CREATE POLICY "Users can update own profile" ON "public"."users" FOR UPDATE USING ((("auth"."uid"())::"text" = ("id")::"text"));



CREATE POLICY "Users can view own listings" ON "public"."listings" FOR SELECT USING ((("auth"."uid"())::"text" = ("user_id")::"text"));



CREATE POLICY "Users can view own profile" ON "public"."users" FOR SELECT USING ((("auth"."uid"())::"text" = ("id")::"text"));



ALTER TABLE "public"."listing_views" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."listings" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."users" ENABLE ROW LEVEL SECURITY;




ALTER PUBLICATION "supabase_realtime" OWNER TO "postgres";


GRANT USAGE ON SCHEMA "public" TO "postgres";
GRANT USAGE ON SCHEMA "public" TO "anon";
GRANT USAGE ON SCHEMA "public" TO "authenticated";
GRANT USAGE ON SCHEMA "public" TO "service_role";






















































































































































GRANT ALL ON FUNCTION "public"."get_trending_cars"("limit_count" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."get_trending_cars"("limit_count" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_trending_cars"("limit_count" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."import_listing"("p_brand" "text", "p_model" "text", "p_year" integer, "p_price" integer, "p_original_price" integer, "p_color" "text", "p_mileage" integer, "p_body_type" "text", "p_fuel_type" "text", "p_transmission" "text", "p_location" "text", "p_description" "text", "p_images" "text"[], "p_user_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."import_listing"("p_brand" "text", "p_model" "text", "p_year" integer, "p_price" integer, "p_original_price" integer, "p_color" "text", "p_mileage" integer, "p_body_type" "text", "p_fuel_type" "text", "p_transmission" "text", "p_location" "text", "p_description" "text", "p_images" "text"[], "p_user_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."import_listing"("p_brand" "text", "p_model" "text", "p_year" integer, "p_price" integer, "p_original_price" integer, "p_color" "text", "p_mileage" integer, "p_body_type" "text", "p_fuel_type" "text", "p_transmission" "text", "p_location" "text", "p_description" "text", "p_images" "text"[], "p_user_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."track_car_view"("car_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."track_car_view"("car_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."track_car_view"("car_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."update_updated_at_column"() TO "anon";
GRANT ALL ON FUNCTION "public"."update_updated_at_column"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."update_updated_at_column"() TO "service_role";


















GRANT ALL ON TABLE "public"."listing_views" TO "anon";
GRANT ALL ON TABLE "public"."listing_views" TO "authenticated";
GRANT ALL ON TABLE "public"."listing_views" TO "service_role";



GRANT ALL ON TABLE "public"."listings" TO "anon";
GRANT ALL ON TABLE "public"."listings" TO "authenticated";
GRANT ALL ON TABLE "public"."listings" TO "service_role";



GRANT ALL ON TABLE "public"."users" TO "anon";
GRANT ALL ON TABLE "public"."users" TO "authenticated";
GRANT ALL ON TABLE "public"."users" TO "service_role";









ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "service_role";






ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "service_role";






ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "service_role";































drop extension if exists "pg_net";


  create policy "Authenticated users can upload car images"
  on "storage"."objects"
  as permissive
  for insert
  to public
with check (((bucket_id = 'car-images'::text) AND (auth.role() = 'authenticated'::text)));



  create policy "Public can view car images"
  on "storage"."objects"
  as permissive
  for select
  to public
using ((bucket_id = 'car-images'::text));



