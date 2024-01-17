from typing import List

from fastapi.responses import StreamingResponse
from llama_index.chat_engine.types import BaseChatEngine

from app.engine.index import get_chat_engine
from fastapi import APIRouter, Depends, HTTPException, Request, status
from llama_index.llms.base import ChatMessage
from llama_index.llms.types import MessageRole
from pydantic import BaseModel
from llama_index.prompts import MessageRole, PromptTemplate

chat_router = r = APIRouter()


class _Message(BaseModel):
    role: MessageRole
    content: str


class _ChatData(BaseModel):
    messages: List[_Message]


@r.post("")
async def chat(
    request: Request,
    data: _ChatData,
    chat_engine: BaseChatEngine = Depends(get_chat_engine),
):
    # check preconditions and get last message
    if len(data.messages) == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No messages provided",
        )
    lastMessage = data.messages.pop()
    if lastMessage.role != MessageRole.USER:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Last message must be from user",
        )
    # convert messages coming from the request to type ChatMessage

    query_instructions =  '''
    Description of the tables in our educational database:
    1. **University Table:**
    - **Columns:** 'uni_name,' 'location,' 'founded,' 'website,' 'overall_ranking,' 'International_Students,' 'Female_Male_Ratio,' 'total_students,' 'athletics,' 'contact,' 'research_funding,' 'airport_transportation,' 'bus_availability,' 'train_station_distance,' 'nearby_shopping_areas,' 'campus_facilities,' 'emergency_services,' 'student_housing,' 'Living costs,' 'student_clubs_organizations,' 'Public_Private.'
    - **Overview:** Comprehensive details about universities, including rankings, demographics, and facilities.
    - **Field Descriptions:**
        - 'uni_name': University name.
        - 'location': Geographical location.
        - 'founded': Founding date.
        - 'website': University website.
        - 'overall_ranking': Overall ranking.
        - 'International_Students': Number of international students.
        - 'Female_Male_Ratio': Ratio of female to male students.
        - 'total_students': Total number of students.
        - 'athletics': Athletics information.
        - 'contact': Contact details.
        - 'research_funding': Research funding details.
        - 'airport_transportation': Accessibility to airports.
        - 'bus_availability': Availability of bus transportation.
        - 'train_station_distance': Distance to train stations.
        - 'nearby_shopping_areas': Proximity to shopping areas.
        - 'campus_facilities': Facilities available on campus.
        - 'emergency_services': Emergency services information.
        - 'student_housing': Student housing details.
        - 'Living costs': Cost of living information.
        - 'student_clubs_organizations': Student clubs and organizations.
        - 'Public_Private': Public or private designation.

    2. **Programme Table:**
    - **Columns:** 'programme_name,' 'duration,' 'description,' 'fees (annual),' 'admission_requirements,' 'degree_awarded,' 'mode_of_study,' 'on_off_campus,' 'scholarships,' 'language_of_instruction,' 'internship_opportunities,' 'study_abroad_opportunities.'
    - **Overview:** In-depth insights into academic programs, covering duration, fees, and admission criteria.
    - **Field Descriptions:**
        - 'programme_name': Program name.
        - 'duration': Program duration.
        - 'description': Program description.
        - 'fees (annual)': Annual fees.
        - 'admission_requirements': Admission criteria.
        - 'degree_awarded': Degree awarded upon completion.
        - 'mode_of_study': Mode of study (e.g., full-time, part-time).
        - 'on_off_campus': On-campus or off-campus status.
        - 'scholarships': Available scholarships.
        - 'language_of_instruction': Language of instruction.
        - 'internship_opportunities': Opportunities for internships.
        - 'study_abroad_opportunities': Opportunities for studying abroad.

    3. **ProgrammeDescription Table:**
    - **Columns:** 'programme_name,' 'overview,' 'website,' 'learning_objectives,' 'program_structure,' 'specialisations,' 'career_opportunities.'
    - **Overview:** Additional context for academic programs, including overviews, learning objectives, and career opportunities.
    - **Field Descriptions:**
        - 'programme_name': Program name.
        - 'overview': Program overview.
        - 'website': Program website.
        - 'learning_objectives': Learning objectives.
        - 'program_structure': Structure of the program.
        - 'specialisations': Specializations available.
        - 'career_opportunities': Career opportunities associated with the program.

    4. **CourseDescription Table:**
    - **Columns:** 'programme_name,' 'course_name,' 'course_description,' 'course_objectives,' 'core_elective.'
    - **Overview:** Breakdown of individual courses within academic programs, including detailed descriptions and core/elective categorization.
    - **Field Descriptions:**
        - 'programme_name': Program name.
        - 'course_name': Course name.
        - 'course_description': Course description.
        - 'course_objectives': Objectives of the course.
        - 'core_elective': Core or elective status.

    5. **TestType Table:**
    - **Columns:** 'programme_name,' 'test_name,' 'average_score,' 'minimum_score.'
    - **Overview:** Captures data related to tests associated with academic programs, including average and minimum scores.
    - **Field Descriptions:**
        - 'programme_name': Program name.
        - 'test_name': Test name.
        - 'average_score': Average test score.
        - 'minimum_score': Minimum required test score.

    6. **Query Instructions:**
    - Enclose table names in double quotes (e.g., `SELECT * FROM "<TABLE_NAME>"`).
    - Try to provide as much relevant information as possible with the correct joins.
    - Utilize `ILIKE` for flexible matching instead of strict equality.
    - Consider an `OR` condition to match both 'programme_name' and 'description.'
    - Before selecting columns, specify the tables explicitly. For example:
        ```sql
        SELECT "University"."uni_name", "Programme"."programme_name", "Programme"."description"
        FROM "University"
        INNER JOIN "Programme" ON "University"."uni_name" = "Programme"."uni_name"
        WHERE "Programme"."description" ILIKE '%computers%';
        ```

    - When answering questions:
        - For questions related to universities and programs, use INNER JOINs on 'uni_name' and 'programme_name.'
        Example: 
        ```sql
        SELECT "University"."uni_name", "University"."location", "University"."founded", "Programme"."programme_name", "Programme"."description"
        FROM "University"
        INNER JOIN "Programme" ON "University"."uni_name" = "Programme"."uni_name";
        ```

        - To incorporate details from 'ProgrammeDescription,' utilize LEFT JOINs on 'programme_name.'
        Example: 
        ```sql
        SELECT "Programme"."programme_name", "Programme"."description", "ProgrammeDescription"."overview"
        FROM "Programme"
        LEFT JOIN "ProgrammeDescription" ON "Programme"."programme_name" = "ProgrammeDescription"."programme_name";
        ```

        - When querying about courses, employ INNER JOINs on 'programme_name.'
        Example: 
        ```sql
        SELECT "Programme"."programme_name", "CourseDescription"."course_name", "CourseDescription"."course_description"
        FROM "Programme"
        INNER JOIN "CourseDescription" ON "Programme"."programme_name" = "CourseDescription"."programme_name";
        ```

        - For test-related queries, utilize INNER JOINs on 'programme_name.'
        Example: 
        ```sql
        SELECT "Programme"."programme_name", "TestType"."test_name", "TestType"."average_score"
        FROM "Programme"
        INNER JOIN "TestType" ON "Programme"."programme_name" = "TestType"."programme_name";
        ```

        - Tailor your JOINs based on the specific question to ensure optimal data retrieval.
    - When displaying programs, ensure to join with 'ProgrammeDescription' for additional relevant information.
    - Present information in bulleted lists for clarity and readability.
    '''

    messages = [
        ChatMessage(
            role=m.role,
            content=m.content,
        )
        for m in data.messages
    ]

    

    prompt = f'{lastMessage.content}{query_instructions}'
    # print(lastMessage.content)
    # print(prompt)
    # query chat engine
    response = chat_engine.stream_chat(ChatMessage(role = MessageRole.USER, content = prompt).content if lastMessage.content.lower() != "hello" else lastMessage.content, messages)

    # stream response
    async def event_generator():
        for token in response.response_gen:
            # If client closes connection, stop sending events
            if await request.is_disconnected():
                break
            yield token

    return StreamingResponse(event_generator(), media_type="text/plain")

### result = 'init'. -> result = StreamingResponse(event_generator(), media_type="text/plain"). -> after 30 sec, whether result got changed? 200 : 403