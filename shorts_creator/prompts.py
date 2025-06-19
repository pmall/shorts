def EVALUATION_PROMPT_TEMPLATE(
    stories_content: list[str], categories: list[str], target_audiences: list[str]
):
    return f"""
You are an expert content creator who specializes in viral short-form video content for entertainment purposes. Your task is to evaluate Reddit stories for their potential to become captivating, ENTERTAINING viral short videos that people would enjoy watching and sharing.

IMPORTANT: You are creating content for ENTERTAINMENT, not therapy or serious discussion. Stories must be suitable for light, engaging video content.

AUTOMATICALLY SCORE 0-10 (unsuitable) for stories involving:
- Suicide, self-harm, or mental health crises
- Drug addiction, substance abuse, or overdoses
- Sexual abuse, domestic violence, or serious trauma
- Death of family members or serious illness
- Deep personal confessions about serious life problems
- Stories that are too heavy, dark, or depressing for entertainment
- Content that would make viewers uncomfortable rather than entertained

GOOD CONTENT for viral short videos includes:
- Funny workplace mishaps or awkward situations
- Light relationship drama with clear resolution
- Satisfying revenge stories (petty, not harmful)
- Embarrassing but harmless moments
- Wholesome family interactions
- Clever solutions to everyday problems
- Mild mysteries or plot twists
- Humorous misunderstandings
- Feel-good moments and positive outcomes

For each story, consider these factors:
- Is this ENTERTAINING rather than heavy or depressing?
- Would this make people smile, laugh, or feel satisfied?
- Does it have emotional hooks without being traumatic?
- Clear conflict/resolution or narrative arc
- Relatability without being too personal/intimate
- Surprising elements or satisfying twists
- Visual storytelling potential for short video format
- Shareability - would people want to share this for fun?

Rate each story on a scale of 0-1000 where:
- 0-10: Unsuitable for entertainment content (too heavy, dark, personal, or inappropriate)
- 11-20: Poor viral potential, boring but not harmful
- 21-40: Below average, some elements but lacking entertainment value
- 41-60: Average potential, decent story but common
- 61-80: Good potential, engaging and entertaining with viral elements
- 81-100: Excellent potential, highly entertaining and viral-worthy

Remember: We're making ENTERTAINMENT content, not documentaries about serious life issues.

Also categorize each story and identify the target audience.
categories: {", ".join(categories)}
target audiences: {", ".join(target_audiences)}

Make sure to evaluate all stories of the list.

Stories to evaluate:

---

{"\n\n---\n\n".join(stories_content)}
""".strip()
