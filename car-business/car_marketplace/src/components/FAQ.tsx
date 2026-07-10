'use client';

import { useState } from 'react';

const faqData = [
  {
    question: 'How do I buy a car on Used Cars Philippines?',
    answer: 'Simply browse our listings, contact the seller through Messenger or phone, and arrange a viewing. Our platform connects buyers and sellers directly.',
  },
  {
    question: 'Are the cars inspected before listing?',
    answer: 'While we encourage sellers to provide accurate information, we recommend buyers inspect the vehicle personally and, if needed, hire a professional mechanic before purchasing.',
  },
  {
    question: 'Can I negotiate the price?',
    answer: 'Yes! Prices listed are usually negotiable. Contact the seller directly to discuss the price.',
  },
  {
    question: 'How do I sell my car?',
    answer: 'Click on "Sell Your Car" in the navigation, fill out the form with your car details, upload photos, and submit. Our admin team will review and approve your listing.',
  },
  {
    question: 'Is there a fee to list my car?',
    answer: 'Currently, listing your car on Used Cars Philippines is completely free!',
  },
];

export default function FAQ() {
  const [openIndex, setOpenIndex] = useState<number | null>(null);

  return (
    <section className="mt-16 bg-gray-50 p-8 rounded-xl">
      <h2 className="text-2xl font-bold mb-6 text-center">Frequently Asked Questions</h2>
      <div className="max-w-3xl mx-auto space-y-4">
        {faqData.map((faq, index) => (
          <div key={index} className="bg-white rounded-lg shadow-sm">
            <button
              onClick={() => setOpenIndex(openIndex === index ? null : index)}
              className="w-full text-left px-6 py-4 flex justify-between items-center"
            >
              <span className="font-medium">{faq.question}</span>
              <span className="text-gray-500">
                {openIndex === index ? '−' : '+'}
              </span>
            </button>
            {openIndex === index && (
              <div className="px-6 pb-4 text-gray-600">
                {faq.answer}
              </div>
            )}
          </div>
        ))}
      </div>
    </section>
  );
}
