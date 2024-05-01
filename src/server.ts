import fastify from "fastify";
import {z} from "zod";
import { sql } from "./lib/postgres";
import postgres from "postgres";
import { redis } from "./lib/redis";

const app = fastify()

app.get('/:code', async (req, rep) =>{
  const getLinkSchema = z.object({
    code: z.string().min(3),
  })

  const {code} = getLinkSchema.parse(req.params)

  const result = await sql/*sql*/`
    SELECT id,original_url
    FROM short_links 
    WHERE short_links.code = ${code}
  `

  if(result.length === 0){
    return rep.status(400).send({message: 'Link not found'})
  }

  await redis.zIncrBy('metrics',1,String(result[0].id))

  return rep.redirect(301,result[0].original_url)
})

app.get('/api/links', async () =>{
  const result = await sql/*sql*/`
    SELECT *
    FROM short_links
    ORDER BY created_at DESC
  `

  return result
})

app.post('/api/links', async (req, rep)=>{
  const createLinkSchema = z.object({
    code: z.string().min(3),
    url: z.string().url(),
  })

  const {code , url} = createLinkSchema.parse(req.body)

  try{
    const result = await sql/*sql*/`
      INSERT INTO short_links (code , original_url)
      VALUES (${code}, ${url})
      RETURNING id
    `

    return rep.status(201).send({shortLinkId: result[0].id})
  }catch(err){
    if(err instanceof postgres.PostgresError){
      if(err.code === '23505'){
        return rep.status(400).send({
          message: 'Duplicated code!'
        })
      }
    }

    console.error(err)
    return rep.status(500).send({
      message: 'Internal error.'
    })
  }
})

app.get('/api/metrics', async () =>{
  const result = await redis.zRangeByScoreWithScores('metrics', 0 , 50)

  const metrics = result.sort((a,b) => b.score - a.score).map(item =>{
    return {
      shortLinkId: Number(item.value),
      clicks: item.score
    }
  })

  return metrics
})

app.listen({
  port: 3333,
}).then(()=>{
  console.log('Http server running')
})