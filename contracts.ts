import { z } from 'zod';

export const OwnerSetupSchema = z.object({
  username: z.string().regex(/^[a-z0-9][a-z0-9._-]{2,79}$/i),
  displayName: z.string().trim().min(1).max(80),
  password: z.string().min(12).max(1024),
});
export const LoginSchema = z.object({username:z.string().min(3).max(80),password:z.string().min(1).max(1024)});
export const PasswordChangeSchema = z.object({currentPassword:z.string().min(1).max(1024),newPassword:z.string().min(12).max(1024)});
export const AuthUserSchema = z.object({id:z.string().uuid(),username:z.string(),displayName:z.string()});
export const AuthResponseSchema = z.object({user:AuthUserSchema});
const PublicSourceInputSchema = z.object({adapter:z.literal('public-html').optional(),name:z.string().trim().min(1).max(120),baseUrl:z.string().url(),termsUrl:z.string().url(),allowedPaths:z.array(z.string().startsWith('/')).min(1),minIntervalSeconds:z.number().int().min(60).max(86400),policyReviewDue:z.string().date(),termsReviewed:z.literal(true),robotsReviewed:z.literal(true)});
const AnsSourceInputSchema = z.object({adapter:z.literal('ans-fhir'),name:z.string().trim().min(1).max(120).optional(),policyReviewDue:z.string().date(),termsReviewed:z.literal(true)});
export const SourceInputSchema = z.union([PublicSourceInputSchema,AnsSourceInputSchema]);
export const PolicyReviewSchema = z.object({termsUrl:z.string().url(),policyReviewDue:z.string().date(),termsReviewed:z.literal(true),robotsReviewed:z.literal(true).optional()});
export const RecordReviewSchema = z.object({status:z.enum(['resolved','verified','flagged']),note:z.string().max(2000).optional()});
export const ProjectStatusSchema = z.enum(['planning','active','paused','completed']);
export const RunStatusSchema = z.enum(['scheduled','queued','running','needs_review','completed','failed','cancelled']);
export const EventTypeSchema = z.enum(['milestone','scrape_run','source_review','quality_review','team_task','follow_up']);
export const ProjectInputSchema = z.object({
  name: z.string().trim().min(1).max(180), ownerId: z.string().min(1), purpose: z.string().max(4000).optional(),
  priority: z.enum(['low','normal','high']).optional(), status: ProjectStatusSchema.optional(),
  startDate: z.string().date().nullable().optional(), endDate: z.string().date().nullable().optional(),
  timezone: z.string().min(1).optional(), notes: z.string().max(10000).optional(),
  targetVolume: z.number().int().nonnegative().nullable().optional(), cadence: z.string().max(200).nullable().optional(),
});
export const ProjectSourceInputSchema = z.object({
  allowedPaths: z.array(z.string().startsWith('/')).min(1),
  selectedFields: z.array(z.enum(['name','role','title','company','location','professional_url','industry','seniority','public_bio','professional_id','specialty','practice_address','practice_phone','practice_email'])).min(1),
  pacingSeconds: z.number().int().positive(), policyReviewDue: z.string().date().optional(),
  query: z.record(z.string(),z.string().max(4000)).optional(),
});
export const RunInputSchema = z.object({
  sourceId: z.string().min(1), scheduledAt: z.string().datetime({offset:true}).nullable().optional(),
  idempotencyKey: z.string().min(8).max(200),
  search: z.record(z.string(),z.string().max(4000)).optional(),
});
export const AgendaInputSchema = z.object({
  title: z.string().trim().min(1).max(240), description: z.string().max(5000).optional(),
  type: EventTypeSchema, startAt: z.string().datetime({offset:true}), endAt: z.string().datetime({offset:true}),
  timezone: z.string().min(1), projectId: z.string().nullable().optional(), sourceId: z.string().nullable().optional(),
  ownerId: z.string().min(1).optional(),
});
export const ReadinessIssueSchema = z.object({code:z.string(),message:z.string(),sourceId:z.string().optional()});
export const ReadinessSchema = z.object({ready:z.boolean(),issues:z.array(ReadinessIssueSchema)});
export const PrepareRecordsInputSchema = z.object({limit:z.number().int().min(1).max(1000).default(1000)});
export const ProjectRecordsQuerySchema = z.object({status:z.enum(['needs_review','verified','resolved','flagged']).optional(),q:z.string().max(200).optional(),limit:z.coerce.number().int().min(1).max(10000).default(10000),offset:z.coerce.number().int().nonnegative().default(0)});
export const PrepareRecordsResponseSchema = z.object({processed:z.number().int().nonnegative(),duplicates:z.number().int().nonnegative(),lowQuality:z.number().int().nonnegative(),remaining:z.number().int().nonnegative(),limit:z.number().int()});
export const ProjectAnalysisSchema = z.object({total:z.number().int().nonnegative(),prepared:z.number().int().nonnegative(),remaining:z.number().int().nonnegative(),duplicates:z.number().int().nonnegative(),lowQuality:z.number().int().nonnegative(),averageQuality:z.number().nullable(),reviewStatuses:z.record(z.string(),z.number()),specialties:z.array(z.object({value:z.string(),count:z.number().int()})),roles:z.array(z.object({value:z.string(),count:z.number().int()})),locations:z.array(z.object({value:z.string(),count:z.number().int()}))});
export const ApiErrorSchema = z.object({error:z.string(),detail:z.string().optional(),message:z.string().optional(),confirmationRequired:z.boolean().optional()});
export type ProjectInput = z.infer<typeof ProjectInputSchema>;
export type OwnerSetup = z.infer<typeof OwnerSetupSchema>;
export type Login = z.infer<typeof LoginSchema>;
export type PasswordChange = z.infer<typeof PasswordChangeSchema>;
export type AuthUser = z.infer<typeof AuthUserSchema>;
export type SourceInput = z.infer<typeof SourceInputSchema>;
export type PolicyReview = z.infer<typeof PolicyReviewSchema>;
export type RecordReview = z.infer<typeof RecordReviewSchema>;
export type ProjectStatus = z.infer<typeof ProjectStatusSchema>;
export type ProjectSourceInput = z.infer<typeof ProjectSourceInputSchema>;
export type RunStatus = z.infer<typeof RunStatusSchema>;
export type RunInput = z.infer<typeof RunInputSchema>;
export type EventType = z.infer<typeof EventTypeSchema>;
export type AgendaInput = z.infer<typeof AgendaInputSchema>;
export type Readiness = z.infer<typeof ReadinessSchema>;
export type PrepareRecordsInput = z.infer<typeof PrepareRecordsInputSchema>;
export type PrepareRecordsResponse = z.infer<typeof PrepareRecordsResponseSchema>;
export type ProjectAnalysis = z.infer<typeof ProjectAnalysisSchema>;
export type ProjectRecordsQuery = z.infer<typeof ProjectRecordsQuerySchema>;
