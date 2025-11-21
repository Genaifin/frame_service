from fastapi import HTTPException
from athena.question_answering import answer_question
from athena.fund_processor import process_data
from athena.db_operations import get_source_id_from_name, get_available_funds

async def athenaResponse(params: dict):
    question = params['query'].get('question')
    # Extract fund name from params if provided
    source_name = params['query'].get('fund_name')

    print(f"source_name is : {source_name}")
    
    if not question:
        raise HTTPException(status_code=400, detail="Question is required")
    
    # Handle fund filtering if fund_name is provided
    fund_id = None
    validated_fund_name = None
    
    if source_name:
        # Map fund name to fund_id (case-insensitive)
        fund_id = get_source_id_from_name(source_name)
        validated_fund_name = source_name     
    
    # Answer the question with optional fund filtering
    response = await answer_question(question, fund_id=fund_id, fund_name=validated_fund_name)

    # Never raise HTTP errors back to the frontend; return a friendly body instead
    if "error" in response:
        fallback_text = response.get("visualization", {}).get("text") or "No results found for your question."
        fallback_modules = response.get("visualization", {}).get("modules", [])
        return {
            "text": fallback_text,
            "isUser": False,
            "isLoading": False,
            "modules": fallback_modules
        }

    # Return multiple-module JSON as-is
    return response
