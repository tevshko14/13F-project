import { Webhook } from "svix";
import { headers } from "next/headers";
import { createClient } from "@supabase/supabase-js";
import type { WebhookEvent } from "@clerk/nextjs/server";

// Service-role client bypasses RLS for webhook inserts
const supabaseAdmin = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_KEY!
);

export async function POST(req: Request) {
  const WEBHOOK_SECRET = process.env.CLERK_WEBHOOK_SECRET;

  if (!WEBHOOK_SECRET) {
    return new Response("Webhook secret not configured", { status: 500 });
  }

  // Verify the webhook signature
  const headerPayload = await headers();
  const svixId = headerPayload.get("svix-id");
  const svixTimestamp = headerPayload.get("svix-timestamp");
  const svixSignature = headerPayload.get("svix-signature");

  if (!svixId || !svixTimestamp || !svixSignature) {
    return new Response("Missing svix headers", { status: 400 });
  }

  const payload = await req.json();
  const body = JSON.stringify(payload);

  const wh = new Webhook(WEBHOOK_SECRET);
  let evt: WebhookEvent;

  try {
    evt = wh.verify(body, {
      "svix-id": svixId,
      "svix-timestamp": svixTimestamp,
      "svix-signature": svixSignature,
    }) as WebhookEvent;
  } catch (err) {
    console.error("Webhook verification failed:", err);
    return new Response("Invalid signature", { status: 400 });
  }

  const eventType = evt.type;

  if (eventType === "user.created" || eventType === "user.updated") {
    const { id, email_addresses, first_name, last_name, image_url } = evt.data;

    const email = email_addresses?.[0]?.email_address ?? null;
    const displayName =
      [first_name, last_name].filter(Boolean).join(" ") || null;

    const { error } = await supabaseAdmin.from("profiles").upsert(
      {
        id,
        email,
        display_name: displayName,
        avatar_url: image_url ?? null,
      },
      { onConflict: "id" }
    );

    if (error) {
      console.error("Profile upsert failed:", error);
      return new Response("Database error", { status: 500 });
    }
  }

  if (eventType === "user.deleted") {
    const { id } = evt.data;

    if (id) {
      const { error } = await supabaseAdmin
        .from("profiles")
        .delete()
        .eq("id", id);

      if (error) {
        console.error("Profile delete failed:", error);
        return new Response("Database error", { status: 500 });
      }
    }
  }

  return new Response("OK", { status: 200 });
}
