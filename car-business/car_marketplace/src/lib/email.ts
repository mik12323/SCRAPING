// Email notification service - Supabase Edge Functions or API route
// For now, we'll create an API route that can be called from server components

export interface EmailData {
  to: string;
  subject: string;
  text: string;
  html?: string;
}

// This is a placeholder - in production, you'd use:
// 1. Supabase Edge Functions with Resend/SendGrid
// 2. Next.js API route with nodemailer
// 3. Third-party service like Resend

export async function sendEmail(emailData: EmailData): Promise<boolean> {
  try {
    // Call an API route or Supabase Edge Function
    const response = await fetch('/api/send-email', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(emailData),
    });

    return response.ok;
  } catch (error) {
    console.error('Error sending email:', error);
    return false;
  }
}

export function generateNewListingEmail(brand: string, model: string, year: number, price: number, username: string, phone: string, listingId: string) {
  const listingUrl = `https://usedcars.ph/car/${listingId}`;
  const adminUrl = `https://usedcars.ph/admin`;

  return {
    to: 'koylito3@gmail.com', // ADMIN_EMAIL from Flask app
    subject: `New Car Listing: ${year} ${brand} ${model}`,
    text: `
New car listing submitted:

Brand: ${brand}
Model: ${model}
Year: ${year}
Price: ₱${price.toLocaleString()}
Seller: ${username}
Phone: ${phone}

View listing: ${listingUrl}
Admin dashboard: ${adminUrl}
    `,
    html: `
<h2>New Car Listing Submitted</h2>
<p><strong>Brand:</strong> ${brand}</p>
<p><strong>Model:</strong> ${model}</p>
<p><strong>Year:</strong> ${year}</p>
<p><strong>Price:</strong> ₱${price.toLocaleString()}</p>
<p><strong>Seller:</strong> ${username}</p>
<p><strong>Phone:</strong> ${phone}</p>
<p><a href="${listingUrl}">View Listing</a></p>
<p><a href="${adminUrl}">Admin Dashboard</a></p>
    `,
  };
}
