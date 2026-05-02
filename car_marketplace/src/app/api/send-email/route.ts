import { NextRequest, NextResponse } from 'next/server';

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { to, subject, text } = body;

    if (!to || !subject || !text) {
      return NextResponse.json(
        { error: 'Missing required fields' },
        { status: 400 }
      );
    }

    // For now, just log the email (Resend API key not configured)
    console.log('Email notification (not sent - no API key):', { to, subject, text });

    // TODO: Configure RESEND_API_KEY in .env.local and uncomment below
    // const { Resend } = await import('resend');
    // const resend = new Resend(process.env.RESEND_API_KEY);
    // const data = await resend.emails.send({...});

    return NextResponse.json({ success: true, message: 'Email logged (not sent)' });
  } catch (error: any) {
    console.error('Error sending email:', error);
    return NextResponse.json(
      { error: 'Failed to send email' },
      { status: 500 }
    );
  }
}
