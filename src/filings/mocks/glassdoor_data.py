"""Mock Glassdoor API response for UI development.

# TODO: Replace with API fetch once UI is finalized.

This mock data mirrors the structure returned by the RapidAPI
"real-time-glassdoor-data" company-overview endpoint for Apple (AAPL).
Used to build and style the Vitals tab without consuming API quota.

Toggle: Set USE_MOCK_GLASSDOOR=1 env var to inject mock data
instead of calling the real API.
"""

MOCK_GLASSDOOR_AAPL: dict = {
    "company_name": "Apple",
    "overall_rating": 4.2,
    "ceo_name": "Tim Cook",
    "ceo_approval_pct": 89,
    "recommend_to_friend_pct": 81,
    "business_outlook_pct": 74,
    "review_count": 45378,
    "logo_url": "https://media.glassdoor.com/sql/1138/apple-squarelogo-1630407773284.png",
    "headquarters": "Cupertino, CA",
    "website": "https://www.apple.com",
    "company_size": "10000+ Employees",
    "industry": "Computer Hardware Development",
    "company_type": "Company - Public",
    "revenue": "$10+ billion (USD)",
    "year_founded": 1976,
    "stock_ticker": "AAPL",
    # Sub-ratings (1.0 - 5.0 scale)
    "culture_and_values_rating": 4.1,
    "work_life_balance_rating": 3.6,
    "senior_management_rating": 3.6,
    "compensation_and_benefits_rating": 4.2,
    "career_opportunities_rating": 3.7,
    "diversity_and_inclusion_rating": 4.3,
    # Glassdoor links
    "glassdoor_url": "https://www.glassdoor.com/Overview/Working-at-Apple-EI_IE1138.11,16.htm",
    "reviews_url": "https://www.glassdoor.com/Reviews/Apple-Reviews-E1138.htm",
    # Timestamp
    "_fetched_at": "2026-02-19T10:30:00",
}
