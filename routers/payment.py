from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import os

router = APIRouter(prefix="/api/payment", tags=["payment"])

# Stripe configuration (to be set via environment variables)
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_PRICE_ID = os.getenv("STRIPE_PRICE_ID", "")


class CheckoutRequest(BaseModel):
    success_url: str
    cancel_url: str


class CheckoutResponse(BaseModel):
    checkout_url: str
    session_id: str


@router.post("/create-checkout", response_model=CheckoutResponse)
async def create_checkout_session(request: CheckoutRequest):
    """
    Create a Stripe checkout session for upgrading to paid tier.

    Note: This is a placeholder implementation. In production:
    1. Set STRIPE_SECRET_KEY and STRIPE_PRICE_ID environment variables
    2. Install stripe package: pip install stripe
    3. Uncomment the Stripe integration code below
    """
    if not STRIPE_SECRET_KEY:
        raise HTTPException(
            status_code=503,
            detail="Payment system not configured. Please contact support."
        )

    # Placeholder response for development
    # In production, uncomment and use the Stripe code below:
    """
    import stripe
    stripe.api_key = STRIPE_SECRET_KEY

    try:
        session = stripe.checkout.Session.create(
            payment_method_types=['card'],
            line_items=[{
                'price': STRIPE_PRICE_ID,
                'quantity': 1,
            }],
            mode='payment',
            success_url=request.success_url + '?session_id={CHECKOUT_SESSION_ID}',
            cancel_url=request.cancel_url,
        )
        return CheckoutResponse(
            checkout_url=session.url,
            session_id=session.id
        )
    except stripe.error.StripeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    """

    # Development placeholder
    return CheckoutResponse(
        checkout_url="https://stripe.com/checkout/placeholder",
        session_id="dev_session_placeholder"
    )


@router.get("/verify/{session_id}")
async def verify_payment(session_id: str):
    """
    Verify a Stripe payment session and return an access token.

    In production, this would:
    1. Verify the session with Stripe
    2. Generate a proper JWT token
    3. Store the payment record
    """
    if not STRIPE_SECRET_KEY:
        raise HTTPException(
            status_code=503,
            detail="Payment system not configured."
        )

    # Placeholder for development
    # In production, verify with Stripe and generate proper token
    """
    import stripe
    stripe.api_key = STRIPE_SECRET_KEY

    try:
        session = stripe.checkout.Session.retrieve(session_id)
        if session.payment_status == 'paid':
            # Generate JWT token for paid user
            token = generate_paid_token(session.customer_email)
            return {"status": "paid", "token": f"paid_{token}"}
        else:
            return {"status": "pending"}
    except stripe.error.StripeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    """

    return {
        "status": "development_mode",
        "message": "Payment verification not available in development mode",
        "token": "paid_dev_token"  # This token enables paid tier in development
    }


@router.get("/pricing")
async def get_pricing():
    """Get current pricing information."""
    return {
        "free_tier": {
            "price": 0,
            "features": [
                "Process up to 100 rows",
                "Basic data cleaning",
                "Validation reports",
                "Visual insights",
                "Watermarked exports"
            ]
        },
        "paid_tier": {
            "price": 9.99,
            "currency": "USD",
            "billing": "one-time",
            "features": [
                "Unlimited rows",
                "Advanced data cleaning",
                "Full validation reports",
                "All visual insights",
                "No watermarks",
                "Priority processing"
            ]
        }
    }
